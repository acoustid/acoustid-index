import os
import requests
import pytest
import subprocess
import time
from urllib.parse import urljoin


# Import the DockerComposeManager for unified testing
class DockerComposeManager:
    """Manages docker compose test environment"""

    def __init__(self):
        self.compose_file = "docker-compose.test.yml"
        self.project_name = "fpindex-test"

    def start(self, *services):
        """Start services via docker compose"""
        cmd = [
            "docker", "compose",
            "-f", self.compose_file,
            "-p", self.project_name,
            "up", "-d", "--build"
        ] + list(services)

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"Failed to start services: {result.stderr}")

        # Wait for all services to be healthy
        self.wait_for_healthy(*services)

    def stop_all(self, remove_volumes=True):
        """Stop and remove all services"""
        # Check if we should keep containers running for debugging
        keep_containers = os.getenv("TEST_KEEP_CONTAINERS", "false").lower() in ("true", "1", "yes")

        if keep_containers:
            print(f"Keeping containers running for debugging. Project: {self.project_name}")
            print(f"To stop manually: docker compose -f {self.compose_file} -p {self.project_name} down -v")
            return

        cmd = [
            "docker", "compose",
            "-f", self.compose_file,
            "-p", self.project_name,
            "down"
        ]

        if remove_volumes:
            cmd.append("-v")

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Failed to stop containers: {result.stderr}")

    def kill(self, *services):
        cmd = [
            "docker", "compose",
            "-f", self.compose_file,
            "-p", self.project_name,
            "kill"
        ] + list(services)

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Failed to kill containers: {result.stderr}")

    def restart(self, *services):
        cmd = [
            "docker", "compose",
            "-f", self.compose_file,
            "-p", self.project_name,
            "restart"
        ] + list(services)

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Failed to restart containers: {result.stderr}")

    def wait_for_healthy(self, *services, timeout=10):
        """Wait for all services to report healthy"""
        deadline = time.time() + timeout

        while time.time() < deadline:
            # Check service health via docker compose ps
            cmd = [
                "docker", "compose",
                "-f", self.compose_file,
                "-p", self.project_name,
                "ps"
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)

            if result.returncode == 0:
                healthy_count = 0
                lines = result.stdout.splitlines()

                for line in lines:
                    print(repr(line))
                    # Check if line contains a service and (healthy) status
                    if any(svc in line for svc in services) and "(healthy)" in line:
                        healthy_count += 1

                if healthy_count >= len(services):
                    return

            time.sleep(0.5)

        # Show logs if startup failed
        self.show_logs()
        raise RuntimeError(f"Services not healthy after {timeout}s")

    def show_logs(self):
        """Show logs from all services for debugging"""
        cmd = [
            "docker", "compose",
            "-f", self.compose_file,
            "-p", self.project_name,
            "logs", "--tail=20"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        print("=== Docker Compose Logs ===")
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)

    def get_service_url(self, service_name, internal_port=6081):
        """Get the external URL for a service"""
        if service_name == "nats":
            return "nats://localhost:14222"
        elif service_name == "fpindex-cluster-1":
            return "http://localhost:16081"
        elif service_name == "fpindex-cluster-2":
            return "http://localhost:16082"
        elif service_name == "fpindex-cluster-3":
            return "http://localhost:16083"
        elif service_name == "fpindex":
            return "http://localhost:6081"
        else:
            raise ValueError(f"Unknown service: {service_name}")


class Server:

    def __init__(self, manager: DockerComposeManager) -> None:
        self.manager = manager

    def get_url(self) -> str:
        return self.manager.get_service_url("fpindex")

    def start(self) -> None:
        self.manager.start('fpindex')

    def restart(self, kill=False) -> None:
        if kill:
            self.manager.kill('fpindex')
        self.manager.restart('fpindex')

    def wait_for_healthy(self, timeout=10) -> None:
        self.manager.wait_for_healthy('fpindex', timeout=timeout)


@pytest.fixture(scope='session')
def docker_compose():
    """Unified server fixture that works with both Docker and subprocess"""
    manager = DockerComposeManager()
    try:
        yield manager
    finally:
        manager.stop_all()


@pytest.fixture(scope='session')
def server(docker_compose):
    """Start fpindex service once per session"""
    srv = Server(docker_compose)
    srv.start()
    return srv


index_no = 1


@pytest.fixture
def index_name(request):
    global index_no
    index_no += 1
    return f't{index_no:03d}'


class Client:
    def __init__(self, session, base_url):
        self.session = session
        self.base_url = base_url

    def head(self, url, **kwargs):
        kwargs.setdefault('timeout', 10)
        return self.session.head(urljoin(self.base_url, url), **kwargs)

    def get(self, url, **kwargs):
        kwargs.setdefault('timeout', 10)
        return self.session.get(urljoin(self.base_url, url), **kwargs)

    def put(self, url, **kwargs):
        kwargs.setdefault('timeout', 10)
        return self.session.put(urljoin(self.base_url, url), **kwargs)

    def post(self, url, **kwargs):
        kwargs.setdefault('timeout', 10)
        return self.session.post(urljoin(self.base_url, url), **kwargs)

    def delete(self, url, **kwargs):
        kwargs.setdefault('timeout', 10)
        return self.session.delete(urljoin(self.base_url, url), **kwargs)


@pytest.fixture
def session():
    with requests.Session() as session:
        yield session

@pytest.fixture
def client(server, session):
    """HTTP client fixture for Docker Compose servers"""
    return Client(session, server.get_url())


@pytest.fixture()
def create_index(client, index_name):
    req = client.put(f'/{index_name}')
    req.raise_for_status()


@pytest.fixture(autouse=True)
def delete_index(client, index_name):
    yield
    req = client.delete(f'/{index_name}')
    req.raise_for_status()
