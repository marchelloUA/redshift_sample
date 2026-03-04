import os
import glob
import pytest


def test_sql_files_exist_and_have_creates():
    sql_dir = os.path.join(os.path.dirname(__file__), '..', 'sql')
    sql_dir = os.path.abspath(sql_dir)
    files = glob.glob(os.path.join(sql_dir, '*.sql'))
    assert files, "No SQL files found"

    for file in files:
        with open(file, 'r') as f:
            content = f.read().lower()
        assert 'create table' in content or 'create external table' in content, \
            f"{file} does not contain any CREATE statements"


def test_requirements_file():
    fn = os.path.join(os.path.dirname(__file__), '..', 'requirements.txt')
    assert os.path.isfile(fn), "requirements.txt missing"
    with open(fn) as f:
        lines = [l.strip() for l in f if l.strip() and not l.startswith('#')]
    assert len(lines) >= 3, "requirements.txt should list at least 3 packages"


def test_readme_contains_sections():
    fn = os.path.join(os.path.dirname(__file__), '..', 'README.md')
    with open(fn) as f:
        text = f.read().lower()
    assert 'python tools' in text
    assert 'testing & integration' in text
    assert 'requirements' in text
