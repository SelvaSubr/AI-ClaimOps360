"""
EDI X12 837P parser — parses professional healthcare claims into Python dicts.
Segment terminator: ~  |  Element separator: *  |  Sub-element separator: :
"""

from __future__ import annotations

import structlog

log = structlog.get_logger()


def split_segments(edi_content: str) -> list[str]:
    """
    Split raw EDI string into individual segments on the ~ terminator.
    Strips all newlines (EDI files may have \n or \r\n between segments).
    Removes empty segments that appear from trailing terminators.

    Args:
        edi_content: Raw EDI file content as a single string.

    Returns:
        list[str]: Non-empty segments, each stripped of leading/trailing whitespace.
    """
    cleaned = edi_content.replace("\n", "").replace("\r", "").strip()
    return [seg.strip() for seg in cleaned.split("~") if seg.strip()]


def parse_isa_envelope(segments: list[str]) -> dict:
    """
    Parse the ISA interchange envelope segment.
    ISA is always the first segment and has exactly 16 elements (16 * symbols).

    Returns:
        dict: sender_id, receiver_id, interchange_date, interchange_ctrl, version.

    Raises:
        ValueError: if ISA segment has wrong element count or is missing.
    """
    isa_segs = [s for s in segments if s.startswith("ISA")]
    if not isa_segs:
        raise ValueError("EDI file has no ISA segment — not a valid X12 file")
    elements = isa_segs[0].split("*")

    if len(elements) != 17:
        raise ValueError(
            f"ISA has {len(elements) - 1} elements, expected 16 elements. "
            f"File may use non-standard delimiter or be truncated."
        )
    return {
        "sender_id": elements[6].strip(),
        "receiver_id": elements[8].strip(),
        "interchange_date": elements[9].strip(),
        "interchange_ctrl": elements[13].strip(),
        "version": elements[12].strip(),
    }


def extract_nm1_loop(segments: list[str], entity_id_qual: str) -> dict:
    """
    Extract a specific NM1 entity loop from the segment list.
    NM1 segments identify billing providers, rendering providers, subscribers, etc.

    Args:
        segments:       Full list of EDI segments.
        entity_id_qual: NM1 qualifier code. Common values:
                        '85'  — billing provider
                        '82'  — rendering provider
                        'IL'  — insured/subscriber (patient)
                        'QC'  — patient (when different from subscriber)
                        '40'  — payer

    Returns:
        dict: entity_type, last_name, first_name, id_code_qual, id_code.
              All values are stripped strings. Empty string if field absent.
    """
    for seg in segments:
        parts = seg.split("*")
        if parts[0] == "NM1" and len(parts) > 1 and parts[1] == entity_id_qual:
            return {
                "entity_type": parts[2].strip() if len(parts) > 2 else "",
                "last_name": parts[3].strip() if len(parts) > 3 else "",
                "first_name": parts[4].strip() if len(parts) > 4 else "",
                "id_code_qual": parts[8].strip() if len(parts) > 8 else "",
                "id_code": parts[9].strip() if len(parts) > 9 else "",
            }
    return {
        "entity_type": "",
        "last_name": "",
        "first_name": "",
        "id_code_qual": "",
        "id_code": "",
    }


def _parse_transaction_set(tx_segs: list[str]) -> list[dict]:
    """
    Parse all CLM claim loops within a single ST/SE transaction set.

    NM1*85 (billing provider) is scoped to the transaction set — each ST/SE
    carries its own provider HL loop, so we resolve it here rather than at
    file level.

    Args:
        tx_segs: Segments from one ST segment through its matching SE segment
                 (inclusive on both ends).

    Returns:
        list[dict]: One parsed-claim dict per CLM segment, matching Contract A.
    """
    # Billing NPI lives in the provider HL loop inside this transaction set
    billing = extract_nm1_loop(tx_segs, "85")

    clm_indices = [i for i, seg in enumerate(tx_segs) if seg.startswith("CLM")]
    if not clm_indices:
        return []

    claims: list[dict] = []
    for pos, clm_idx in enumerate(clm_indices):
        # Upper bound: position of next CLM's enclosing HL, or end of tx_segs
        end = clm_indices[pos + 1] if pos + 1 < len(clm_indices) else len(tx_segs)

        # Walk back from CLM to the opening HL of this patient/subscriber loop
        start = clm_idx
        for i in range(clm_idx - 1, -1, -1):
            if tx_segs[i].startswith("HL"):
                start = i
                break

        loop_segs = tx_segs[start:end]

        # transaction_id from CLM element 01
        clm_parts = tx_segs[clm_idx].split("*")
        transaction_id = clm_parts[1].strip() if len(clm_parts) > 1 else ""

        # date_of_service from DTP*472 (service date line item)
        date_of_service = ""
        for seg in loop_segs:
            parts = seg.split("*")
            if parts[0] == "DTP" and len(parts) >= 4 and parts[1] == "472":
                date_of_service = parts[3].strip()
                break

        # diagnosis codes from HI segments (ABK/BK = primary, ABF/BF = additional)
        diagnosis_codes: list[str] = []
        for seg in loop_segs:
            if not seg.startswith("HI"):
                continue
            for element in seg.split("*")[1:]:
                if ":" in element:
                    qual, code = element.split(":", 1)
                    if qual.strip() in ("ABK", "BK", "ABF", "BF"):
                        code = code.strip()
                        if code:
                            diagnosis_codes.append(code)

        # procedure codes + billed amount from SV1 segments
        procedure_codes: list[str] = []
        billed_amount = 0.0
        for seg in loop_segs:
            if not seg.startswith("SV1"):
                continue
            parts = seg.split("*")
            if len(parts) > 1 and ":" in parts[1]:
                code = parts[1].split(":")[1].strip()
                if code:
                    procedure_codes.append(code)
            elif len(parts) > 1 and parts[1].strip():
                procedure_codes.append(parts[1].strip())
            if len(parts) > 3:
                try:
                    billed_amount += float(parts[3].strip())
                except ValueError:
                    log.warning("sv1_billed_parse_failed", seg=seg[:40])

        # prior auth from REF*D9
        prior_auth: str | None = None
        for seg in loop_segs:
            parts = seg.split("*")
            if parts[0] == "REF" and len(parts) > 2 and parts[1] == "D9":
                prior_auth = parts[2].strip() or None
                break

        patient = extract_nm1_loop(loop_segs, "IL")
        rendering = extract_nm1_loop(loop_segs, "82")

        parsed = {
            "transaction_id": transaction_id,
            "billing_npi": billing["id_code"],
            "rendering_npi": rendering["id_code"],
            "patient_member_id": patient["id_code"],
            "date_of_service": date_of_service,
            "diagnosis_codes": diagnosis_codes,
            "procedure_codes": procedure_codes,
            "billed_amount": round(billed_amount, 2),
            "prior_auth_number": prior_auth,
        }
        log.info(
            "edi_parsed",
            transaction_id=transaction_id,
            npi=billing["id_code"],
            diagnoses=len(diagnosis_codes),
            billed=billed_amount,
        )
        claims.append(parsed)

    return claims


def parse_837_multi(edi_content: str) -> list[dict]:
    """
    Parse every CLM transaction in an EDI 837P file.

    Handles the full X12 envelope hierarchy:
      - One ISA/IEA interchange (validated on entry)
      - Up to 17 GS/GE functional groups per ISA
      - Up to 5,000 CLM segments per ST/SE transaction set
      - Up to 85,000 claims per file in the extreme case

    Each ST/SE transaction set is processed independently so that per-set
    context (e.g. NM1*85 billing provider) is correctly scoped.  A file
    with a single claim is handled identically to one with 85,000.

    Args:
        edi_content: Raw EDI file content as a single string.

    Returns:
        list[dict]: One parsed-claim dict per CLM segment, matching Contract A.
                    May be a single-element list for single-claim files.

    Raises:
        ValueError: if ISA envelope is invalid.
    """
    segments = split_segments(edi_content)
    parse_isa_envelope(segments)

    # Locate ST/SE transaction-set boundaries.
    # Zip pairs them: ST[0]→SE[0], ST[1]→SE[1], …
    st_indices = [i for i, seg in enumerate(segments) if seg.startswith("ST")]
    se_indices = [i for i, seg in enumerate(segments) if seg.startswith("SE")]

    if not st_indices or len(st_indices) != len(se_indices):
        # Malformed or no ST/SE wrapper — treat entire file as one transaction set
        log.warning(
            "edi_st_se_mismatch",
            st_count=len(st_indices),
            se_count=len(se_indices),
        )
        return _parse_transaction_set(segments)

    all_claims: list[dict] = []
    for st_idx, se_idx in zip(st_indices, se_indices):
        tx_segs = segments[st_idx : se_idx + 1]
        all_claims.extend(_parse_transaction_set(tx_segs))

    return all_claims


def parse_837(edi_content: str) -> dict:
    """Full parse of a single-claim EDI X12 837P transaction. Returns Contract A dict.

    Raises:
        ValueError: if ISA envelope is invalid.
    """
    segments = split_segments(edi_content)
    parse_isa_envelope(segments)

    # transaction_id from CLM element 01
    clm_parts = next((s.split("*") for s in segments if s.startswith("CLM")), [])
    transaction_id = clm_parts[1].strip() if len(clm_parts) > 1 else ""

    # date_of_service from DTP*472
    date_of_service = ""
    for seg in segments:
        parts = seg.split("*")
        if parts[0] == "DTP" and len(parts) >= 4 and parts[1] == "472":
            date_of_service = parts[3].strip()
            break

    # diagnosis codes from HI segments (ABK/BK = primary, ABF/BF = additional)
    diagnosis_codes: list[str] = []
    for seg in segments:
        if not seg.startswith("HI"):
            continue
        for element in seg.split("*")[1:]:
            if ":" in element:
                qual, code = element.split(":", 1)
                if qual.strip() in ("ABK", "BK", "ABF", "BF"):
                    code = code.strip()
                    if code:
                        diagnosis_codes.append(code)

    # procedure codes + billed amount from SV1; element 01 format: 'HC:99213' or plain CPT
    procedure_codes: list[str] = []
    billed_amount = 0.0
    for seg in segments:
        if not seg.startswith("SV1"):
            continue
        parts = seg.split("*")
        if len(parts) > 1 and ":" in parts[1]:
            code_parts = parts[1].split(":")
            code = code_parts[1].strip()  # index 1 = CPT, index 2 = modifier (ignored)
            if code:
                procedure_codes.append(code)
        elif len(parts) > 1 and parts[1].strip():
            procedure_codes.append(parts[1].strip())
        if len(parts) > 3:
            try:
                billed_amount += float(parts[3].strip())
            except ValueError:
                log.warning("sv1_billed_parse_failed", element=parts[2], seg=seg[:40])

    # prior auth from REF*D9
    prior_auth: str | None = None
    for seg in segments:
        parts = seg.split("*")
        if parts[0] == "REF" and len(parts) > 2 and parts[1] == "D9":
            prior_auth = parts[2].strip() or None
            break

    billing = extract_nm1_loop(segments, "85")
    patient = extract_nm1_loop(segments, "IL")
    rendering = extract_nm1_loop(segments, "82")

    result = {
        "transaction_id": transaction_id,
        "billing_npi": billing["id_code"],
        "rendering_npi": rendering["id_code"],
        "patient_member_id": patient["id_code"],
        "date_of_service": date_of_service,
        "diagnosis_codes": diagnosis_codes,
        "procedure_codes": procedure_codes,
        "billed_amount": round(billed_amount, 2),
        "prior_auth_number": prior_auth,
    }
    log.info(
        "edi_parsed",
        transaction_id=transaction_id,
        npi=billing["id_code"],
        diagnoses=len(diagnosis_codes),
        billed=billed_amount,
    )
    return result
