import pytest
import business_glossary_import_v2

def get_mock_logger():
        class MockLogger:
            def __init__(self):
                self.logs = []
            def info(self, msg, *a, **kw):
                self.logs.append(msg)
            def warning(self, msg, *a, **kw):
                self.logs.append(msg)
        return MockLogger()

def test_get_referenced_scopes_related_link_type(monkeypatch):
    monkeypatch.setattr(business_glossary_import_v2, "get_link_type", lambda *a, **kw: "related")
    monkeypatch.setattr(business_glossary_import_v2, "get_project_scopes_from_all_lines", lambda *a, **kw: set(["projects/abc", "projects/def"]))

    main_project_id = "main123"
    file_path = "dummy.txt"

    result = business_glossary_import_v2.get_referenced_scopes(file_path, main_project_id)
    assert f"projects/{main_project_id}" in result
    assert "projects/abc" in result
    assert "projects/def" in result
    assert len(result) == 3

def test_get_referenced_scopes_synonym_link_type(monkeypatch):
    monkeypatch.setattr(business_glossary_import_v2, "get_link_type", lambda *a, **kw: "synonym")
    monkeypatch.setattr(business_glossary_import_v2, "get_project_scopes_from_all_lines", lambda *a, **kw: set(["projects/xyz"]))

    main_project_id = "main456"
    file_path = "dummy.txt"

    result = business_glossary_import_v2.get_referenced_scopes(file_path, main_project_id)
    assert f"projects/{main_project_id}" in result
    assert "projects/xyz" in result
    assert len(result) == 2

@pytest.mark.parametrize("is_empty", [True, False])
def test_process_import_file_empty(monkeypatch, is_empty):
    # Setup
    monkeypatch.setattr(business_glossary_import_v2, "is_file_empty", lambda f: is_empty)
    monkeypatch.setattr(business_glossary_import_v2, "move_file_to_imported_folder", lambda f: None)
    monkeypatch.setattr(business_glossary_import_v2, "get_dataplex_service", lambda: "service")
    monkeypatch.setattr(business_glossary_import_v2, "build_payload", lambda *a, **kw: ("jobid", {"payload": True}, "location"))
    monkeypatch.setattr(business_glossary_import_v2, "prepare_gcs_bucket", lambda *a, **kw: True)
    monkeypatch.setattr(business_glossary_import_v2, "create_and_monitor_job", lambda *a, **kw: True)

    result = business_glossary_import_v2.process_import_file("file.txt", "proj", "bucket", "migration_folder_1")
    if is_empty:
        assert result is True
    else:
        assert result is True  # Should succeed with all mocks returning True

def test_process_import_file_payload_missing(monkeypatch):
    monkeypatch.setattr(business_glossary_import_v2, "is_file_empty", lambda f: False)
    monkeypatch.setattr(business_glossary_import_v2, "get_dataplex_service", lambda: "service")
    # Simulate missing payload/job_id/job_location
    monkeypatch.setattr(business_glossary_import_v2, "build_payload", lambda *a, **kw: (None, None, None))

    result = business_glossary_import_v2.process_import_file("file.txt", "proj", "bucket", "migration_folder_1")
    assert result is False

def test_process_import_file_gcs_upload_failed(monkeypatch):
    monkeypatch.setattr(business_glossary_import_v2, "is_file_empty", lambda f: False)
    monkeypatch.setattr(business_glossary_import_v2, "get_dataplex_service", lambda: "service")
    monkeypatch.setattr(business_glossary_import_v2, "build_payload", lambda *a, **kw: ("jobid", {"payload": True}, "location"))
    monkeypatch.setattr(business_glossary_import_v2, "prepare_gcs_bucket", lambda *a, **kw: False)

    result = business_glossary_import_v2.process_import_file("file.txt", "proj", "bucket", "migration_folder_1")
    assert result is False

def test_process_import_file_job_failed(monkeypatch):
    monkeypatch.setattr(business_glossary_import_v2, "is_file_empty", lambda f: False)
    monkeypatch.setattr(business_glossary_import_v2, "get_dataplex_service", lambda: "service")
    monkeypatch.setattr(business_glossary_import_v2, "build_payload", lambda *a, **kw: ("jobid", {"payload": True}, "location"))
    monkeypatch.setattr(business_glossary_import_v2, "prepare_gcs_bucket", lambda *a, **kw: True)
    monkeypatch.setattr(business_glossary_import_v2, "create_and_monitor_job", lambda *a, **kw: False)

    result = business_glossary_import_v2.process_import_file("file.txt", "proj", "bucket", "migration_folder_1")
    assert result is False

def test_process_import_file_exception(monkeypatch):
    monkeypatch.setattr(business_glossary_import_v2, "is_file_empty", lambda f: False)
    monkeypatch.setattr(business_glossary_import_v2, "get_dataplex_service", lambda: "service")
    monkeypatch.setattr(business_glossary_import_v2, "build_payload", lambda *a, **kw: ("jobid", {"payload": True}, "location"))
    monkeypatch.setattr(business_glossary_import_v2, "prepare_gcs_bucket", lambda *a, **kw: True)
    def raise_exc(*a, **kw): raise Exception("fail")
    monkeypatch.setattr(business_glossary_import_v2, "create_and_monitor_job", raise_exc)

    result = business_glossary_import_v2.process_import_file("file.txt", "proj", "bucket", "migration_folder_1")
    assert result is False


def test_run_import_files_no_files(monkeypatch):
    files = []
    project_id = "proj"
    buckets = ["bucket"]
    result = business_glossary_import_v2.run_import_files(files, project_id, buckets)
    assert result == []

def test_run_import_files_no_buckets(monkeypatch):
    files = ["file1.txt", "file2.txt"]
    project_id = "proj"
    buckets = []
    result = business_glossary_import_v2.run_import_files(files, project_id, buckets)
    assert result == [False, False]

def test_run_import_files_single_bucket_folder_based(monkeypatch):
    """Test that files are assigned to different folders within a single bucket."""
    monkeypatch.setattr(business_glossary_import_v2, "ensure_folders_exist", lambda b, f: True)
    monkeypatch.setattr(business_glossary_import_v2, "import_files_with_threads", lambda pid, b, assignments, results, mw: results.extend([True, True]))
    
    files = ["file1.txt", "file2.txt"]
    project_id = "proj"
    buckets = ["bucketA"]
    
    result = business_glossary_import_v2.run_import_files(files, project_id, buckets)
    assert len(result) == 2
    assert result == [True, True]

def test_run_import_files_multiple_buckets_uses_first(monkeypatch):
    """Test that only first bucket is used when multiple buckets provided."""
    monkeypatch.setattr(business_glossary_import_v2, "ensure_folders_exist", lambda b, f: True)
    monkeypatch.setattr(business_glossary_import_v2, "import_files_with_threads", lambda pid, b, assignments, results, mw: results.extend([True, True]))
    
    files = ["file1.txt", "file2.txt"]
    project_id = "proj"
    buckets = ["bucketA", "bucketB"]
    
    result = business_glossary_import_v2.run_import_files(files, project_id, buckets)
    assert len(result) == 2

def test_run_import_files_folder_creation_fails(monkeypatch):
    """Test that import fails if folder creation fails."""
    monkeypatch.setattr(business_glossary_import_v2, "ensure_folders_exist", lambda b, f: False)
    
    files = ["file1.txt", "file2.txt"]
    project_id = "proj"
    buckets = ["bucketA"]
    
    result = business_glossary_import_v2.run_import_files(files, project_id, buckets)
    assert result == [False, False]

def test_import_files_with_threads_success(monkeypatch):
    """Test import_files_with_threads with successful imports."""
    results = []
    call_count = [0]
    
    def mock_process_import_file(file_path, project_id, bucket, folder_name):
        call_count[0] += 1
        return True
    
    monkeypatch.setattr(business_glossary_import_v2, "process_import_file", mock_process_import_file)
    
    assignments = [("file1.txt", "migration_folder_1"), ("file2.txt", "migration_folder_2")]
    project_id = "proj"
    bucket = "bucketA"
    max_workers = 2
    
    business_glossary_import_v2.import_files_with_threads(project_id, bucket, assignments, results, max_workers)
    assert len(results) == 2
    assert all(results)

def test_import_files_with_threads_partial_failure(monkeypatch):
    """Test import_files_with_threads with partial failures."""
    results = []
    
    def mock_process_import_file(file_path, project_id, bucket, folder_name):
        return file_path == "file1.txt"
    
    monkeypatch.setattr(business_glossary_import_v2, "process_import_file", mock_process_import_file)
    
    assignments = [("file1.txt", "migration_folder_1"), ("file2.txt", "migration_folder_2")]
    project_id = "proj"
    bucket = "bucketA"
    max_workers = 2
    
    business_glossary_import_v2.import_files_with_threads(project_id, bucket, assignments, results, max_workers)
    assert len(results) == 2
    assert results.count(True) == 1
    assert results.count(False) == 1

def test_import_files_with_threads_keyboard_interrupt(monkeypatch):
    """Test import_files_with_threads handles keyboard interrupt."""
    results = []
    
    def mock_process_import_file(file_path, project_id, bucket, folder_name):
        if file_path == "file2.txt":
            raise KeyboardInterrupt()
        return True
    
    monkeypatch.setattr(business_glossary_import_v2, "process_import_file", mock_process_import_file)
    
    assignments = [("file1.txt", "migration_folder_1"), ("file2.txt", "migration_folder_2")]
    project_id = "proj"
    bucket = "bucketA"
    max_workers = 2
    
    with pytest.raises(KeyboardInterrupt):
        business_glossary_import_v2.import_files_with_threads(project_id, bucket, assignments, results, max_workers)



def test_filter_files_for_phases_entrylinks_all_pass(monkeypatch):
    # All files pass dependency check
    monkeypatch.setattr(business_glossary_import_v2, "check_entrylink_dependency", lambda f: True)
    files = ["file1.txt", "file2.txt"]
    result = business_glossary_import_v2.filter_files_for_phases("EntryLinks", files)
    assert result == files

def test_filter_files_for_phases_entrylinks_some_pass(monkeypatch):
    # Only some files pass dependency check
    monkeypatch.setattr(business_glossary_import_v2, "check_entrylink_dependency", lambda f: f == "file1.txt")
    files = ["file1.txt", "file2.txt"]
    result = business_glossary_import_v2.filter_files_for_phases("EntryLinks", files)
    assert result == ["file1.txt"]

def test_filter_files_for_phases_entrylinks_none_pass(monkeypatch):
    # No files pass dependency check
    monkeypatch.setattr(business_glossary_import_v2, "check_entrylink_dependency", lambda f: False)
    files = ["file1.txt", "file2.txt"]
    result = business_glossary_import_v2.filter_files_for_phases("EntryLinks", files)
    assert result == []

def test_filter_files_for_phases_non_entrylinks(monkeypatch):
    # Should return all files unchanged for non-EntryLinks phase
    files = ["file1.txt", "file2.txt"]
    result = business_glossary_import_v2.filter_files_for_phases("Glossaries", files)
    assert result == files

def test_filter_files_for_phases_entrylinks_empty_list(monkeypatch):
    # Empty input list should return empty list
    monkeypatch.setattr(business_glossary_import_v2, "check_entrylink_dependency", lambda f: True)
    files = []
    result = business_glossary_import_v2.filter_files_for_phases("EntryLinks", files)
    assert result == []

def test_process_phase_no_files(monkeypatch):
    logs = []
    class Logger:
        def info(self, msg, *args, **kwargs):
            logs.append(msg)
        def warning(self, msg, *args, **kwargs):
            logs.append(msg)
    monkeypatch.setattr("business_glossary_import_v2.logger", Logger())
    business_glossary_import_v2.process_phase("Glossaries", [], "proj", ["bucket"])
    # Should log that there are no files to process
    assert any("No files found in Glossaries folder. Skipping phase." in msg for msg in logs)

def test_process_phase_all_success(monkeypatch):
    mock_logger = get_mock_logger()
    monkeypatch.setattr("business_glossary_import_v2.logger", mock_logger)
    monkeypatch.setattr(business_glossary_import_v2, "filter_files_for_phases", lambda phase, files: files)
    monkeypatch.setattr(business_glossary_import_v2, "run_import_files", lambda files, pid, buckets: [True, True])

def test_process_phase_some_failures(monkeypatch):
    mock_logger = get_mock_logger()
    monkeypatch.setattr("business_glossary_import_v2.logger", mock_logger)
    monkeypatch.setattr(business_glossary_import_v2, "filter_files_for_phases", lambda phase, files: files)
    monkeypatch.setattr(business_glossary_import_v2, "run_import_files", lambda files, pid, buckets: [True, False])

def test_process_phase_filtered_files(monkeypatch):
    mock_logger = get_mock_logger()
    monkeypatch.setattr("business_glossary_import_v2.logger", mock_logger)
    # Only one file passes filter
    monkeypatch.setattr(business_glossary_import_v2, "filter_files_for_phases", lambda phase, files: [files[0]])
    monkeypatch.setattr(business_glossary_import_v2, "run_import_files", lambda files, pid, buckets: [True])

def test_process_phase_all_fail(monkeypatch):
    logs = []
    class MockLogger:
        def info(self, msg, *args, **kwargs):
            logs.append(msg)
        def warning(self, msg, *args, **kwargs):
            logs.append(msg)
    mock_logger = MockLogger()
    monkeypatch.setattr("business_glossary_import_v2.logger", mock_logger)
    monkeypatch.setattr(business_glossary_import_v2, "filter_files_for_phases", lambda phase, files: files)
    monkeypatch.setattr(business_glossary_import_v2, "run_import_files", lambda files, pid, buckets: [False, False])
    files = ["file1.txt", "file2.txt"]
    business_glossary_import_v2.process_phase("Glossaries", files, "proj", ["bucket"])
    assert any("0/2 files imported successfully" in msg for msg in logs)
    assert any("failed to import" in msg for msg in logs)
    

def test_main_returns_true_when_no_files(monkeypatch):
    # Both directories return empty lists
    monkeypatch.setattr("business_glossary_import_v2.get_file_paths_from_directory", lambda path: [])
    monkeypatch.setattr("business_glossary_import_v2.GLOSSARIES_DIRECTORY_PATH", "GLOSSARIES_DIRECTORY_PATH")
    monkeypatch.setattr("business_glossary_import_v2.ENTRYLINKS_DIRECTORY_PATH", "ENTRYLINKS_DIRECTORY_PATH")
    monkeypatch.setattr(business_glossary_import_v2, "process_phase", lambda *a, **kw: None)
    result = business_glossary_import_v2.main("proj", ["bucket"])
    assert result is True

def test_main_returns_false_when_files_exist(monkeypatch):
    # At least one directory returns files
    monkeypatch.setattr("business_glossary_import_v2.get_file_paths_from_directory", lambda path: ["file.txt"] if path == "GLOSSARIES_DIRECTORY_PATH" else [])
    monkeypatch.setattr("business_glossary_import_v2.GLOSSARIES_DIRECTORY_PATH", "GLOSSARIES_DIRECTORY_PATH")
    monkeypatch.setattr("business_glossary_import_v2.ENTRYLINKS_DIRECTORY_PATH", "ENTRYLINKS_DIRECTORY_PATH")
    monkeypatch.setattr(business_glossary_import_v2, "process_phase", lambda *a, **kw: None)
    result = business_glossary_import_v2.main("proj", ["bucket"])
    assert result is False

def test_main_calls_process_phase_for_each_phase(monkeypatch):
    called = []
    monkeypatch.setattr("business_glossary_import_v2.get_file_paths_from_directory", lambda path: ["f1.txt"] if path == "GLOSSARIES_DIRECTORY_PATH" else ["e1.txt"])
    monkeypatch.setattr("business_glossary_import_v2.GLOSSARIES_DIRECTORY_PATH", "GLOSSARIES_DIRECTORY_PATH")
    monkeypatch.setattr("business_glossary_import_v2.ENTRYLINKS_DIRECTORY_PATH", "ENTRYLINKS_DIRECTORY_PATH")
    def mock_process_phase(phase_name, files, project_id, buckets):
        called.append((phase_name, list(files), project_id, list(buckets)))
    monkeypatch.setattr(business_glossary_import_v2, "process_phase", mock_process_phase)
    monkeypatch.setattr(business_glossary_import_v2, "import_status", lambda: True)
    result = business_glossary_import_v2.main("proj", ["bucket"])
    assert ("Glossaries", ["f1.txt"], "proj", ["bucket"]) in called
    assert ("EntryLinks", ["e1.txt"], "proj", ["bucket"]) in called
    assert result is True

def test_main_with_multiple_files_and_buckets(monkeypatch):
    gloss_files = ["g1.txt", "g2.txt"]
    entry_files = ["e1.txt", "e2.txt"]
    monkeypatch.setattr("business_glossary_import_v2.get_file_paths_from_directory", lambda path: gloss_files if path == "GLOSSARIES_DIRECTORY_PATH" else entry_files)
    monkeypatch.setattr("business_glossary_import_v2.GLOSSARIES_DIRECTORY_PATH", "GLOSSARIES_DIRECTORY_PATH")
    monkeypatch.setattr("business_glossary_import_v2.ENTRYLINKS_DIRECTORY_PATH", "ENTRYLINKS_DIRECTORY_PATH")
    called = []
    def mock_process_phase(phase_name, files, project_id, buckets):
        called.append((phase_name, list(files), project_id, list(buckets)))
    monkeypatch.setattr(business_glossary_import_v2, "process_phase", mock_process_phase)
    monkeypatch.setattr(business_glossary_import_v2, "import_status", lambda: False)
    result = business_glossary_import_v2.main("proj", ["bucketA", "bucketB"])
    assert ("Glossaries", gloss_files, "proj", ["bucketA", "bucketB"]) in called
    assert ("EntryLinks", entry_files, "proj", ["bucketA", "bucketB"]) in called
    assert result is False

def test_import_status_returns_true_when_no_files(monkeypatch):
    # Both directories return empty lists
    monkeypatch.setattr("business_glossary_import_v2.get_file_paths_from_directory", lambda path: [])
    monkeypatch.setattr("business_glossary_import_v2.GLOSSARIES_DIRECTORY_PATH", "GLOSSARIES_DIRECTORY_PATH")
    monkeypatch.setattr("business_glossary_import_v2.ENTRYLINKS_DIRECTORY_PATH", "ENTRYLINKS_DIRECTORY_PATH")
    result = business_glossary_import_v2.import_status()
    assert result is True

def test_import_status_returns_false_when_glossaries_exist(monkeypatch):
    # Glossaries directory returns files, entrylinks is empty
    monkeypatch.setattr("business_glossary_import_v2.get_file_paths_from_directory", lambda path: ["file.txt"] if path == "GLOSSARIES_DIRECTORY_PATH" else [])
    monkeypatch.setattr("business_glossary_import_v2.GLOSSARIES_DIRECTORY_PATH", "GLOSSARIES_DIRECTORY_PATH")
    monkeypatch.setattr("business_glossary_import_v2.ENTRYLINKS_DIRECTORY_PATH", "ENTRYLINKS_DIRECTORY_PATH")
    result = business_glossary_import_v2.import_status()
    assert result is False

def test_import_status_returns_false_when_entrylinks_exist(monkeypatch):
    # Entrylinks directory returns files, glossaries is empty
    monkeypatch.setattr("business_glossary_import_v2.get_file_paths_from_directory", lambda path: ["file.txt"] if path == "ENTRYLINKS_DIRECTORY_PATH" else [])
    monkeypatch.setattr("business_glossary_import_v2.GLOSSARIES_DIRECTORY_PATH", "GLOSSARIES_DIRECTORY_PATH")
    monkeypatch.setattr("business_glossary_import_v2.ENTRYLINKS_DIRECTORY_PATH", "ENTRYLINKS_DIRECTORY_PATH")
    result = business_glossary_import_v2.import_status()
    assert result is False

def test_import_status_returns_false_when_both_exist(monkeypatch):
    # Both directories return files
    monkeypatch.setattr("business_glossary_import_v2.get_file_paths_from_directory", lambda path: ["file.txt"])
    monkeypatch.setattr("business_glossary_import_v2.GLOSSARIES_DIRECTORY_PATH", "GLOSSARIES_DIRECTORY_PATH")
    monkeypatch.setattr("business_glossary_import_v2.ENTRYLINKS_DIRECTORY_PATH", "ENTRYLINKS_DIRECTORY_PATH")
    result = business_glossary_import_v2.import_status()
    assert result is False








