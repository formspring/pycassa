from pycassa.system_manager import SystemManager
from pycassa.logging import pycassa_logger

class Discover(object):
    """Autodiscover the server list for a given keyspace.
    The per keyspace is not really important but that's how describe_ring works.

    Caveat: I have made some tests with instances running on a different port than 9160 and I got None for EndpointDetails.port.
    If you plan on using this with cassandra running on a port other than 9160, make your own tests.
    """

    def __init__(self, keyspace, servers=None):
        super(Discover, self).__init__()

        if not servers: servers = ['localhost:9160']
        self.servers = servers
        self.keyspace = keyspace

        self.root_logger = pycassa_logger.PycassaLogger()
        self.logger = self.root_logger.add_child_logger('discover', self.name_changed)

    def name_changed(self, new_logger):
        self.logger = new_logger

    def get_ring(self):
        """
        Get the ring by calling describe_ring
        """
        ring = None
        for server in self.servers:
            try:
                system_manager = SystemManager(server)
                ring = system_manager.describe_ring(keyspace=self.keyspace)
            except Exception as e:
                print e
                self.logger.error("Can't describe ring for %s on %s: %s", self.keyspace, server, e)

        if ring is None:
            return ring

        seen = set()
        for token_range in ring:
            host = token_range.endpoint_details[0].host
            port = token_range.endpoint_details[0].port or 9160
            server = "%s:%s" % (host, port)
            if server in seen:
                continue
            if self.validate(server):
                seen.add(server)
        return list(seen)

    def validate(self, host):
        """
        This can be used to validate if the server is healthy and active in the ring as opposed to Leaving / Joining / Unreachable / Moving.
        Unfortunately this does not seem to be exposed in the thrift IDL, just JMX
        """
        return True