import run
import types

def test_all_exports_successful(monkeypatch):
    call_results = [True, True, True]

    def mock_run_export_worker(url, user_project, org_ids):
        return call_results.pop(0)

    monkeypatch.setattr(run, "run_export_worker", mock_run_export_worker)

    class DummyLogger:
        def info(self, msg): pass

    monkeypatch.setattr(run, "logger", DummyLogger())

    glossary_urls = ['url1', 'url2', 'url3']
    user_project = 'test-project'
    org_ids = ['org1', 'org2']

    result = run.perform_exports(glossary_urls, user_project, org_ids)
    assert result == 3


def test_some_exports_fail(monkeypatch):
    call_results = [True, False, True]

    def mock_run_export_worker(url, user_project, org_ids):
        return call_results.pop(0)

    monkeypatch.setattr(run, "run_export_worker", mock_run_export_worker)

    class DummyLogger:
        def info(self, msg): pass

    monkeypatch.setattr(run, "logger", DummyLogger())

    glossary_urls = ['url1', 'url2', 'url3']
    user_project = 'test-project'
    org_ids = ['org1']

    result = run.perform_exports(glossary_urls, user_project, org_ids)
    assert result == 2


def test_perform_exports_empty(monkeypatch):
    class DummyLogger:
        def info(self, msg): pass

    monkeypatch.setattr(run, "logger", DummyLogger())

    result = run.perform_exports([], "test-project", ["org1"])
    assert result == 0


def test_find_glossaries_in_project_transforms_entries(monkeypatch):
    monkeypatch.setattr(run.api_layer, "discover_glossaries", lambda *_: [
        "https://example.com/projects/p1/entries/e1",
        "https://example.com/projects/p1/entries/e2"
    ])

    class DummyLogger:
        def info(self, msg): pass

    monkeypatch.setattr(run, "logger", DummyLogger())

    result = run.find_glossaries_in_project("p1", "user-project")

    assert result == [
        "https://example.com/projects/p1/glossaries/e1",
        "https://example.com/projects/p1/glossaries/e2"
    ]


def test_find_glossaries_in_project_no_entries(monkeypatch):
    monkeypatch.setattr(run.api_layer, "discover_glossaries", lambda *_: [
        "https://example.com/projects/p1/glossaries/g1"
    ])

    class DummyLogger:
        def info(self, msg): pass

    monkeypatch.setattr(run, "logger", DummyLogger())

    result = run.find_glossaries_in_project("p1", "user-project")
    assert result == [
        "https://example.com/projects/p1/glossaries/g1"
    ]


def test_find_glossaries_in_project_empty(monkeypatch):
    monkeypatch.setattr(run.api_layer, "discover_glossaries", lambda *_: [])

    class DummyLogger:
        def info(self, msg): pass

    monkeypatch.setattr(run, "logger", DummyLogger())

    result = run.find_glossaries_in_project("p1", "user-project")
    assert result == []

def test_run_export_worker_success(monkeypatch):
    # Mock execute_export to return True
    monkeypatch.setattr(run, "execute_export", lambda url, user_project, org_ids: True)

    result = run.run_export_worker("url", "user_project", ["org1"])
    assert result is True

def test_run_export_worker_failure(monkeypatch):
    # Mock execute_export to raise an exception
    def mock_execute_export(_, __, ___):
        raise Exception("Export error")
    monkeypatch.setattr(run, "execute_export", mock_execute_export)

    class DummyLogger:
        def error(self, *args, **kwargs): 
            DummyLogger.called = True
        def debug(self, *args, **kwargs):
            pass
    DummyLogger.called = False
    monkeypatch.setattr(run, "logger", DummyLogger())

    result = run.run_export_worker("url", "user_project", ["org1"])
    assert result is False
    assert DummyLogger.called is True

def test_find_glossaries_in_project_with_entries(monkeypatch):
    # Mock discover_glossaries to return URLs with /entries/
    monkeypatch.setattr(run.api_layer, "discover_glossaries", lambda project_id, user_project: [
        "https://example.com/projects/p1/entries/e1",
        "https://example.com/projects/p1/entries/e2"
    ])

    class DummyLogger:
        def info(self, msg): pass
    monkeypatch.setattr(run, "logger", DummyLogger())

    result = run.find_glossaries_in_project("p1", "user-project")
    assert result == [
        "https://example.com/projects/p1/glossaries/e1",
        "https://example.com/projects/p1/glossaries/e2"
    ]

def test_find_glossaries_in_project_with_glossaries(monkeypatch):
    # Mock discover_glossaries to return URLs with /glossaries/
    monkeypatch.setattr(run.api_layer, "discover_glossaries", lambda project_id, user_project: [
        "https://example.com/projects/p1/glossaries/g1"
    ])

    class DummyLogger:
        def info(self, msg): pass
    monkeypatch.setattr(run, "logger", DummyLogger())

    result = run.find_glossaries_in_project("p1", "user-project")
    assert result == [
        "https://example.com/projects/p1/glossaries/g1"
    ]

def test_find_glossaries_in_project_empty(monkeypatch):
    # Mock discover_glossaries to return empty list
    monkeypatch.setattr(run.api_layer, "discover_glossaries", lambda project_id, user_project: [])

    class DummyLogger:
        def info(self, msg): pass
    monkeypatch.setattr(run, "logger", DummyLogger())

    result = run.find_glossaries_in_project("p1", "user-project")
    assert result == []
def test_perform_exports_all_success(monkeypatch):
    # All exports succeed
    results = [True, True, True]
    def mock_run_export_worker(url, user_project, org_ids):
        return results.pop(0)
    monkeypatch.setattr(run, "run_export_worker", mock_run_export_worker)

    class DummyLogger:
        def info(self, msg): pass
    monkeypatch.setattr(run, "logger", DummyLogger())

    glossary_urls = ["url1", "url2", "url3"]
    user_project = "project"
    org_ids = ["org1"]
    assert run.perform_exports(glossary_urls, user_project, org_ids) == 3

def test_perform_exports_some_fail(monkeypatch):
    # Some exports fail
    results = [True, False, True]
    def mock_run_export_worker(url, user_project, org_ids):
        return results.pop(0)
    monkeypatch.setattr(run, "run_export_worker", mock_run_export_worker)

    class DummyLogger:
        def info(self, msg): pass
    monkeypatch.setattr(run, "logger", DummyLogger())

    glossary_urls = ["url1", "url2", "url3"]
    user_project = "project"
    org_ids = ["org1"]
    assert run.perform_exports(glossary_urls, user_project, org_ids) == 2

def test_perform_exports_all_fail(monkeypatch):
    # All exports fail
    results = [False, False, False]
    def mock_run_export_worker(url, user_project, org_ids):
        return results.pop(0)
    monkeypatch.setattr(run, "run_export_worker", mock_run_export_worker)

    class DummyLogger:
        def info(self, msg): pass
    monkeypatch.setattr(run, "logger", DummyLogger())

    glossary_urls = ["url1", "url2", "url3"]
    user_project = "project"
    org_ids = ["org1"]
    assert run.perform_exports(glossary_urls, user_project, org_ids) == 0

def test_perform_exports_empty_list(monkeypatch):
    # No glossaries to export
    class DummyLogger:
        def info(self, msg): pass
    monkeypatch.setattr(run, "logger", DummyLogger())

    glossary_urls = []
    user_project = "project"
    org_ids = ["org1"]
    assert run.perform_exports(glossary_urls, user_project, org_ids) == 0

def test_perform_imports_success(monkeypatch):
    called = {}

    def mock_main(project_id, buckets):
        called['main'] = (project_id, buckets)

    monkeypatch.setattr(run.business_glossary_import_v2, "main", mock_main)

    class DummyLogger:
        def error(self, msg, exc_info=None): 
            called['error'] = True
    monkeypatch.setattr(run, "logger", DummyLogger())

    run.perform_imports("test-project", ["bucket1", "bucket2"])
    assert called['main'] == ("test-project", ["bucket1", "bucket2"])
    assert 'error' not in called

def test_perform_imports_exception(monkeypatch):
    called = {}

    def mock_main(*args, **kwargs):
        raise Exception("Import error")

    monkeypatch.setattr(run.business_glossary_import_v2, "main", mock_main)

    class DummyLogger:
        def error(self, msg, exc_info=None): 
            called['error'] = (msg, True)
        def debug(self, *args, **kwargs): 
            pass
    monkeypatch.setattr(run, "logger", DummyLogger())

    run.perform_imports("test-project", ["bucket1"])
    assert 'error' in called
    assert "An error occurred during the import step:" in called['error'][0]
    assert called['error'][1] is True

def test_main_full_migration(monkeypatch):
    called = {}

    # Mock logging setup
    monkeypatch.setattr(run.logging_utils, "setup_file_logging", lambda: called.setdefault("setup_file_logging", True))
    # Mock log_migration_start
    monkeypatch.setattr(run, "log_migration_start", lambda project_id: called.setdefault("log_migration_start", project_id))
    # Mock export_glossaries
    def mock_export_glossaries(project_id, user_project, org_ids, start_time):
        called["export_glossaries"] = (project_id, user_project, org_ids, start_time)
        return True
    monkeypatch.setattr(run, "export_glossaries", mock_export_glossaries)
    # Mock perform_imports
    monkeypatch.setattr(run, "perform_imports", lambda project_id, buckets: called.setdefault("perform_imports", (project_id, buckets)))
    # Mock time.time
    monkeypatch.setattr(run.time, "time", lambda: 123.45)
    # Mock check_all_buckets_permissions to always return True
    monkeypatch.setattr(run, "check_all_buckets_permissions", lambda buckets, project_number=None: True)
    # Mock api_layer.get_project_number to avoid real API call
    monkeypatch.setattr(run.api_layer, "get_project_number", lambda project_id, user_project=None: "123456789")

    args = types.SimpleNamespace(
        project="proj1",
        buckets=["bucket1", "bucket2"],
        orgIds=["org1", "org2"],
        user_project="user-proj",
        resume_import=False
    )

    run.main(args)

    assert called["setup_file_logging"] is True
    assert called["log_migration_start"] == "proj1"
    assert called["export_glossaries"] == ("proj1", "user-proj", ["org1", "org2"], 123.45)
def test_main_resume_import(monkeypatch):
    called = {}

    monkeypatch.setattr(run.logging_utils, "setup_file_logging", lambda: called.setdefault("setup_file_logging", True))
    monkeypatch.setattr(run, "log_migration_start", lambda project_id: called.setdefault("log_migration_start", project_id))
    # Should NOT call export_glossaries
    monkeypatch.setattr(run, "export_glossaries", lambda *a, **kw: called.setdefault("export_glossaries", True))
    monkeypatch.setattr(run, "perform_imports", lambda project_id, buckets: called.setdefault("perform_imports", (project_id, buckets)))
    monkeypatch.setattr(run.time, "time", lambda: 999.99)
    monkeypatch.setattr(run, "check_all_buckets_permissions", lambda buckets, project_number=None: True)
    # Mock api_layer.get_project_number to avoid real API call
    monkeypatch.setattr(run.api_layer, "get_project_number", lambda project_id, user_project=None: "987654321")

    args = types.SimpleNamespace(
        project="proj2",
        buckets=["bucketA"],
        orgIds=["orgX"],
        user_project="user-proj2",
        resume_import=True
    )

    run.main(args)

    assert called["setup_file_logging"] is True
    assert called["log_migration_start"] == "proj2"
    assert "export_glossaries" not in called
    assert called["perform_imports"] == ("proj2", ["bucketA"])
    assert "export_glossaries" not in called
    assert called["perform_imports"] == ("proj2", ["bucketA"])
    
def test_export_and_validate_glossaries_all_success(monkeypatch):
    # All glossaries found and all exports succeed
    monkeypatch.setattr(run, "find_glossaries_in_project", lambda project_id, user_project: ["url1", "url2"])
    monkeypatch.setattr(run, "perform_exports", lambda glossary_urls, user_project, org_ids: 2)
    monkeypatch.setattr(run, "log_export_summary", lambda successful_exports, total_exports, start_time: None)
    monkeypatch.setattr(run, "all_exports_successful", lambda successful_exports, total_exports: True)

    class DummyLogger:
        def info(self, msg): pass
        def error(self, msg): pass
    monkeypatch.setattr(run, "logger", DummyLogger())

    result = run.export_glossaries("proj", "user_proj", ["org1"], 123.0)
    assert result is True

def test_export_and_validate_glossaries_some_fail(monkeypatch):
    # Glossaries found, some exports fail
    monkeypatch.setattr(run, "find_glossaries_in_project", lambda project_id, user_project: ["url1", "url2", "url3"])
    monkeypatch.setattr(run, "perform_exports", lambda glossary_urls, user_project, org_ids: 2)
    monkeypatch.setattr(run, "log_export_summary", lambda successful_exports, total_exports, start_time: None)
    monkeypatch.setattr(run, "all_exports_successful", lambda successful_exports, total_exports: False)

    class DummyLogger:
        called = False
        def info(self, msg): pass
        def error(self, msg): DummyLogger.called = True
    monkeypatch.setattr(run, "logger", DummyLogger())

    result = run.export_glossaries("proj", "user_proj", ["org1"], 123.0)
    assert result is False
    assert DummyLogger.called is True

def test_export_and_validate_glossaries_none_found(monkeypatch):
    # No glossaries found
    monkeypatch.setattr(run, "find_glossaries_in_project", lambda project_id, user_project: [])
    monkeypatch.setattr(run, "perform_exports", lambda glossary_urls, user_project, org_ids: 0)
    monkeypatch.setattr(run, "log_export_summary", lambda successful_exports, total_exports, start_time: None)
    monkeypatch.setattr(run, "all_exports_successful", lambda successful_exports, total_exports: True)

    class DummyLogger:
        called = False
        def info(self, msg): DummyLogger.called = True
        def error(self, msg): pass
    monkeypatch.setattr(run, "logger", DummyLogger())

    result = run.export_glossaries("proj", "user_proj", ["org1"], 123.0)
    assert result is False
    assert DummyLogger.called is True






