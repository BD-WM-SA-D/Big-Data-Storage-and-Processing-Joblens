"""Kiểm thử phần thuần của spark-jobs/layout_iceberg.py (đếm file ứng viên theo min/max, câu lệnh CALL)."""

from __future__ import annotations

import layout_iceberg as L


def test_overlaps_is_conservative_without_stats():
    assert L.overlaps(None, "9", "5", "5") and L.overlaps("1", None, "5", "5")
    assert L.overlaps("1", "9", "5", "5") and not L.overlaps("6", "9", "5", "5")
    assert not L.overlaps(10, 20, 21, 30) and L.overlaps(10, 20, 20, 30)  # biên kín


def test_sorted_files_cut_candidates():
    """Cùng 4 file, cùng dữ liệu: chưa sắp xếp thì id nào cũng nằm trong khoảng của mọi file; sắp xếp thì 1 file."""
    unsorted = [{"lower": "0", "upper": "99", "bytes": 4 * 2**20} for _ in range(4)]
    sorted_ = [{"lower": a, "upper": b, "bytes": 4 * 2**20} for a, b in [("0", "24"), ("25", "49"), ("50", "74"), ("75", "99")]]  # noqa: E501
    assert L.file_stats(unsorted, "42", "42") == {"files": 4, "mb": 16.0, "candidate_files": 4, "candidate_mb": 16.0}
    assert L.file_stats(sorted_, "42", "42")["candidate_files"] == 1


def test_sql_statements():
    s = L.sort_sql("bronze.jobs_raw", "source_job_id", 4_000_000, "source = 'vietjobs'")
    assert "strategy => 'sort'" in s and "sort_order => 'source_job_id ASC NULLS LAST'" in s
    assert "'target-file-size-bytes', '4000000'" in s and "where => \"source = 'vietjobs'\"" in s
    alter, rewrite = L.evolve_sql("silver.job_postings_history", "months(valid_from)")
    assert alter == "ALTER TABLE lake.silver.job_postings_history ADD PARTITION FIELD months(valid_from)"
    assert "rewrite-all" in rewrite


def test_table_md_has_header_and_rows():
    md = L.table_md([{"table": "t", "probe": "p", "stage": "trước", "files": 1, "mb": 1.0, "candidate_files": 1,
                      "candidate_mb": 1.0}])  # fmt: skip
    assert md.splitlines()[0].startswith("| table | probe | stage") and len(md.splitlines()) == 3
