import pytest

from hsextract_dbos.app.main import workflow_metadata_extraction

def test_example_workflow(reset_dbos):
    file_object_path: str = "sblack/d7b526e24f7e449098b428ae9363f514/data/contents/dataroot.csv"
    file_updated: bool = True
    resource_root_path: str = "sblack/d7b526e24f7e449098b428ae9363f514/data/contents"
    resource_md_root_path: str = "sblack/md/d7b526e24f7e449098b428ae9363f514"
    resource_md_part_path: str = "sblack/.md/d7b526e24f7e449098b428ae9363f514"
    assert workflow_metadata_extraction(file_object_path, file_updated, resource_root_path, resource_md_root_path, resource_md_part_path)