from libprobe.probe import Probe
from lib.check.wmi import check_wmi
from lib.version import __version__ as version


if __name__ == '__main__':
    checks = {
        'wmi': check_wmi
    }

    probe = Probe("wmi", version, checks)

    probe.start()
