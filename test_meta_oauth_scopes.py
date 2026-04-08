import unittest

from webapp.client_oauth_meta import SCOPES as CLIENT_SCOPES
from webapp.meta_scopes import META_SCOPES
from webapp.oauth_meta import SCOPES as ADMIN_SCOPES


class MetaOAuthScopeTests(unittest.TestCase):
    def test_admin_and_client_use_shared_scope_set(self):
        self.assertEqual(ADMIN_SCOPES, META_SCOPES)
        self.assertEqual(CLIENT_SCOPES, META_SCOPES)

    def test_required_warren_meta_scopes_present(self):
        expected = {
            "pages_show_list",
            "pages_read_engagement",
            "pages_read_user_content",
            "pages_manage_posts",
            "pages_manage_metadata",
            "pages_messaging",
            "leads_retrieval",
        }
        self.assertTrue(expected.issubset(set(META_SCOPES)))


if __name__ == "__main__":
    unittest.main()