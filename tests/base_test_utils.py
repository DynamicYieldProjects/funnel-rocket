import os
import tempfile

IN_GITHUB_WORKFLOW = "GITHUB_WORKFLOW" in os.environ
SKIP_SLOW_TESTS = os.environ.get('SKIP_LOCAL_S3_TESTS', False) or IN_GITHUB_WORKFLOW
# noinspection PyProtectedMember,PyUnresolvedReferences
TEMP_DIR = tempfile._get_default_tempdir()


# noinspection PyProtectedMember,PyUnresolvedReferences
def temp_filename(suffix='', with_dir: bool = True):
    fname = next(tempfile._get_candidate_names()) + suffix
    return f"{TEMP_DIR}/{fname}" if with_dir else fname