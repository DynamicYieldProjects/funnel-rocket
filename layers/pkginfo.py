import sys
import argparse
import pkg_resources

# TODO doc

DEFAULT_PACKAGE_NAME = 'funnel-rocket'
LAMBDA_BUILTIN_PACKAGES = ['boto3']

parser = argparse.ArgumentParser(
    description='Get package info',
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('--pkgname', type=str, default=DEFAULT_PACKAGE_NAME,
                    help='Package name')
parser.add_argument('--skip-lambda-builtins', action="store_true",
                    help='Remove packages already provided by AWS Lambda runtime')
args = parser.parse_args()

package_info = pkg_resources.working_set.by_key[args.pkgname]
if not package_info:
    sys.exit(f"Can't find package {args.pkgname}")

reqs = {str(r) for r in package_info.requires()}
if not reqs:
    sys.exit(f"Requires list is empty")

if args.skip_lambda_builtins:
    reqs.difference_update(LAMBDA_BUILTIN_PACKAGES)

print(" ".join(reqs))
