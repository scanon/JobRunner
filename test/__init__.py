import os


def setup():
    print("Module setup")
    test_dir = os.path.dirname(os.path.abspath(__file__))
    os.environ['PATH'] = '%s/bin/:%s' % (test_dir, os.environ['PATH'])
    # Create __init__


def teardown():
    if os.path.exists('ssh.out'):
        os.unlink('ssh.out')
