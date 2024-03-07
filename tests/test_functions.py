from datetime import datetime
from unittest.mock import Mock
from unittest.mock import patch

from eimerdb.functions import get_datetime
from eimerdb.functions import get_initials


def test_get_datetime():
    assert get_datetime().startswith(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


@patch("eimerdb.functions.AuthClient.fetch_local_user_from_jupyter")
def test_get_initials_given_mocked_auth_client(auth_client_mock: Mock):
    auth_client_mock.return_value = {"username": "user42@some.org"}

    assert get_initials() == "user42"

    auth_client_mock.assert_called_once()


def test_get_initials_given_no_auth_client():
    assert get_initials() == "user"
