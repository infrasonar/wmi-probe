from libprobe.probe import Probe
from lib.check.disk import check_disk
from lib.check.disk_io import check_disk_io
from lib.check.installed_software import check_installed_software
from lib.check.ip4_route_table import check_ip4_route_table
from lib.check.logged_on_users import check_logged_on_users
from lib.check.memory import check_memory
from lib.check.network_adapter import check_network_adapter
from lib.check.network_interface import check_network_interface
from lib.check.nt_domain import check_nt_domain
from lib.check.nt_eventlog import check_nt_eventlog
from lib.check.operating_system import check_operating_system
from lib.check.page_file import check_page_file
from lib.check.process import check_process
from lib.check.processor import check_processor
from lib.check.remote_users import check_remote_users
from lib.check.services import check_services
from lib.check.system import check_system
from lib.check.system_time import check_system_time
from lib.check.updates import check_updates
from lib.check.volume import check_volume
from lib.version import __version__ as version


if __name__ == '__main__':
    checks = {
        'disk': check_disk,
        'diskIo': check_disk_io,
        'installedSoftware': check_installed_software,
        'ip4RouteTable': check_ip4_route_table,
        'loggedOnUsers': check_logged_on_users,
        'memory': check_memory,
        'networkAdapter': check_network_adapter,
        'networkInterface': check_network_interface,
        'ntDomain': check_nt_domain,
        'ntEventlog': check_nt_eventlog,
        'operatingSystem': check_operating_system,
        'pageFile': check_page_file,
        'process': check_process,
        'processor': check_processor,
        'remoteUsers': check_remote_users,
        'services': check_services,
        'system': check_system,
        'systemTime': check_system_time,
        'updates': check_updates,
        'volume': check_volume,
    }

    probe = Probe("wmi", version, checks)

    probe.start()
