from unittest import mock

import pytest

from astro.files.locations import create_file_location


class MockCommand:
    def __init__(self, result):
        self.result = result

    def execute(self):
        return self.result


class MockFiles:
    def __init__(self, mock_get, mock_list):
        self.mock_get = mock_get
        self.mock_list = mock_list

    def list(self, **kwargs):  # noqa: A003
        return MockCommand(self.mock_list(**kwargs))

    def get(self, **kwargs):
        return MockCommand(self.mock_get(**kwargs))


class MockConn:
    def __init__(self, *, get, list):
        self.mock_get = get
        self.mock_list = list

    def files(self):
        return MockFiles(self.mock_get, self.mock_list)


def test_size_not_found(monkeypatch):
    location = create_file_location("gdrive:///does-not-exist")

    mock_list = mock.MagicMock()
    mock_get = mock.MagicMock()

    monkeypatch.setattr(location, "_get_conn", lambda: MockConn(list=mock_list, get=mock_get))
    mock_list.side_effect = [{"files": []}]

    with pytest.raises(FileNotFoundError) as ctx:
        location.size
    assert str(ctx.value) == location.path
    assert mock_list.mock_calls == [
        mock.call(q="parents in 'root' and name = 'does-not-exist'", fields="id", pageSize="1"),
    ]
    assert mock_get.mock_calls == []


def test_size_in_root(monkeypatch):
    location = create_file_location("gdrive:///something-in-root")

    mock_list = mock.MagicMock()
    mock_get = mock.MagicMock()

    monkeypatch.setattr(location, "_get_conn", lambda: MockConn(list=mock_list, get=mock_get))
    mock_list.side_effect = [{"files": [{"id": "fake_id"}]}]
    mock_get.side_effect = [{"size": "123"}]

    assert location.size == 123
    assert mock_list.mock_calls == [
        mock.call(q="parents in 'root' and name = 'something-in-root'", fields="id", pageSize="1"),
    ]
    assert mock_get.mock_calls == [mock.call(fileId="fake_id", fields="size")]


def test_size_in_subdir(monkeypatch):
    location = create_file_location("gdrive:///folder/my-file")

    mock_list = mock.MagicMock()
    mock_get = mock.MagicMock()

    monkeypatch.setattr(location, "_get_conn", lambda: MockConn(list=mock_list, get=mock_get))
    mock_list.side_effect = [{"files": [{"id": "folder_id"}]}, {"files": [{"id": "file_id"}]}]
    mock_get.side_effect = [{"size": "456"}]

    assert location.size == 456
    assert mock_list.mock_calls == [
        mock.call(q="parents in 'root' and name = 'folder'", fields="id", pageSize="1"),
        mock.call(q="parents in 'folder_id' and name = 'my-file'", fields="id", pageSize="1"),
    ]
    assert mock_get.mock_calls == [mock.call(fileId="file_id", fields="size")]
