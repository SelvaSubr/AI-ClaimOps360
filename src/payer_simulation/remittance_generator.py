"""src/payer_simulation/remittance_generator.py — EDI X12 835 builder."""

from __future__ import annotations

import datetime
import os

import structlog
from dotenv import load_dotenv

load_dotenv()
load_dotenv(
    dotenv_path=os.path.join(os.path.dirname(__file__), "..", "simulation.config"),
    override=False,
)

log = structlog.get_logger()


def _seg(*elements: str) -> str:
    return "*".join(elements) + "~"


def generate_835(adjudication: dict) -> str:
    """Build EDI 835 string from Contract F adjudication dict."""
    payer_id = os.getenv("PAYER_ID")
    payer_id = payer_id or "UNKNOWN_PAYER"
    submitter_id = os.getenv("SUBMITTER_ID") or "UNKNOWN_SUBMITTER"
    payer_routing = os.getenv("PAYER_ROUTING_NUM") or "000000000"
    provider_routing = os.getenv("PROVIDER_ROUTING_NUM") or "000000000"
    payer_account = os.getenv("PAYER_ACCOUNT_NUM") or "000000000"
    provider_account = os.getenv("PROVIDER_ACCOUNT_NUM") or "000000000"
    patient_last = os.getenv("PATIENT_LAST_NAME") or "UNKNOWN"
    patient_first = os.getenv("PATIENT_FIRST_NAME") or "UNKNOWN"

    now = datetime.datetime.utcnow()
    ds = now.strftime("%Y%m%d")
    ts = now.strftime("%H%M")
    ctrl = now.strftime("%Y%m%d%H%M%S")[-9:]
    paid = f"{adjudication['paid_amount']:.2f}"
    billed = f"{adjudication['billed_amount']:.2f}"
    cas_amt = f"{adjudication['cas_amount']:.2f}"
    cas_c = adjudication["cas_code"]
    clp_st = "1" if adjudication["status"] == "paid" else "4"
    segs = [
        _seg(
            "ISA",
            "00",
            "          ",
            "00",
            "          ",
            "30",
            (payer_id or "UNKNOWN_PAYER").ljust(15),
            "30",
            submitter_id.ljust(15),
            ds[2:],
            ts,
            "^",
            "00501",
            ctrl.zfill(9),
            "0",
            "P",
            ":",
        ),
        _seg("GS", "HP", payer_id, submitter_id, ds, ts, ctrl, "X", "005010X221A1"),
        _seg("ST", "835", ctrl.zfill(4)),
        _seg(
            "BPR",
            "I",
            paid,
            "C",
            "CHK",
            "01",
            payer_routing,
            "DA",
            payer_account,
            "1",
            payer_id,
            "01",
            provider_routing,
            "DA",
            provider_account,
            ds,
        ),
        _seg("TRN", "1", ctrl, payer_id),
        _seg(
            "CLP",
            adjudication["claim_id"],
            clp_st,
            billed,
            paid,
            "",
            payer_id,
            "HC",
            adjudication["claim_id"],
            "11",
        ),
        _seg(
            "NM1",
            "QC",
            "1",
            patient_last,
            patient_first,
            "",
            "",
            "",
            "MI",
            adjudication["member_id"],
        ),
    ]
    if float(cas_amt) > 0:
        parts = cas_c.split("-")
        segs.append(_seg("CAS", parts[0], parts[-1], cas_amt))
    for p in adjudication.get("procedures", []):
        segs.append(_seg("SVC", f"HC:{p['cpt']}", f"{p['allowed']:.2f}", "0"))
    segs += [
        _seg("SE", str(len(segs) + 1), ctrl.zfill(4)),
        _seg("GE", "1", ctrl),
        _seg("IEA", "1", ctrl.zfill(9)),
    ]
    log.info("835_generated", claim_id=adjudication["claim_id"], paid=paid)
    return "\n".join(segs)
