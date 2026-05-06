"""Unit tests for src/payer_simulation/enrollment_parser.py."""

import pytest

from src.payer_simulation.enrollment_parser import parse_834

MOCK_834_PATH = "sample_data/834/mock_enrollment_001.edi"


def test_parse_834_returns_dict():
    result = parse_834(MOCK_834_PATH)
    assert isinstance(result, dict)
    first = next(iter(result.values()))
    for key in ("coverage_start", "coverage_end"):
        assert key in first


def test_parse_834_extracts_member_id():
    result = parse_834(MOCK_834_PATH)
    assert "MCKMEMBR0001" in result


def test_parse_834_invalid_edi_raises(tmp_path):
    bad_file = tmp_path / "bad.edi"
    bad_file.write_text("")
    result = parse_834(str(bad_file))
    assert result == {}
