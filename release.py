import sys
import subprocess

# TODO Script assumes you have ~/.pypirc set up with username + password

def help_and_exit():
  print '''
    Welcome to the Infinispan Python Client Release Script.

    Usage:

        $ release.py <version>

    E.g.,

        $ release.py 1.0.0b1 <-- this will tag off master.
  '''
  sys.exit(0)

def input_with_default(msg, default):
  i = raw_input("%s[%s]: " % msg, default)
  if i.strip() == "":
    i = default
  return i

def run_git(opts):
  call = ['git']
  if type(opts) == list:
    for o in opts:
      call.append(o)
  elif type(opts) == str:
    for o in opts.split(' '):
      if o != '':
        call.append(o)
  else:
    raise Exception("Cannot handle argument of type %s" % type(opts))

  print 'Executing %s' % call
  return subprocess.Popen(call, stdout=subprocess.PIPE).communicate()[0].split('\n')

def tag_for_release(version):
  '''Tags the current branch for release using the tag name.'''
  run_git(["tag", "-a", "-m", "'Release Script: tag %s'" % version, version])

def push_to_origin():
  '''Pushes the updated tags to origin'''
  run_git("push origin --tags")

def upload_to_pypi():
  call = ['python', 'setup.py', 'register', 'sdist', 'upload']
  subprocess.Popen(call, stdout=subprocess.PIPE).communicate()[0].split('\n')

def release():
  # We start by determining whether the version passed in is a valid one
  if len(sys.argv) < 1:
    help_and_exit()

  # base_dir = os.getcwd()
  version = sys.argv[1]
  branch = "master"

  print "Releasing Infinispan Python client version %s from branch '%s'" % (version, branch)
  sure = input_with_default('Are you sure you want to continue?', 'N')
  if not sure.upper().startswith('Y'):
    print '... User Abort!'
    sys.exit(1)
  print 'OK, releasing! Please stand by ...'

  print "Step 1: Tagging %s in git as %s" % (branch, version)
  tag_for_release(version)

  # TODO Update setup.by programmatically and commit
  
  print "Step 2: Push tags" % (branch, version)
  push_to_origin()

  print "Step 3: Upload distribution to PyPI"
  upload_to_pypi()

if __name__ == "__main__":
  release()