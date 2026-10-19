#!/usr/bin/env python3
"""Проверка трёх реальных JVM: репликация, падение лидера и восстановление."""
import json
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
JAR = ROOT / 'build/libs/raft-0.0.1-SNAPSHOT.jar'


def free_ports():
    sockets = [socket.socket() for _ in range(3)]
    try:
        for sock in sockets:
            sock.bind(('127.0.0.1', 0))
        return {f'n{i + 1}': sock.getsockname()[1] for i, sock in enumerate(sockets)}
    finally:
        for sock in sockets:
            sock.close()


def request(port, path, data=None):
    body = None if data is None else json.dumps(data).encode()
    req = urllib.request.Request(f'http://127.0.0.1:{port}{path}', body,
                                 {'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=3) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.load(error)


def eventually(check, description):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (OSError, ValueError):
            pass
        time.sleep(.1)
    raise AssertionError(f'Timeout: {description}')


def main():
    if not JAR.exists():
        raise SystemExit('Run ./gradlew bootJar first')
    ports = free_ports()
    processes = {}
    files = []
    with tempfile.TemporaryDirectory(prefix='raft-smoke-') as directory:
        def start(node):
            output = open(Path(directory) / f'{node}-{len(files)}.log', 'w')
            files.append(output)
            args = ['java', '-Xmx128m', '-jar', str(JAR), f'--server.port={ports[node]}',
                    f'--raft.node-id={node}']
            for index, peer in enumerate(peer for peer in ports if peer != node):
                args += [f'--raft.peers[{index}].id={peer}',
                         f'--raft.peers[{index}].url=http://127.0.0.1:{ports[peer]}']
            processes[node] = subprocess.Popen(args, cwd=ROOT, stdout=output, stderr=subprocess.STDOUT)

        def stop(node):
            process = processes.pop(node)
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)

        def leader():
            statuses = [request(ports[node], '/status')[1] for node in processes]
            leaders = [status['nodeId'] for status in statuses if status['role'] == 'LEADER']
            if len(leaders) == 1 and all(status['leaderId'] == leaders[0] for status in statuses):
                return leaders[0]
            return None

        def logs_equal():
            logs = [request(ports[node], '/log')[1] for node in processes]
            return logs[0] if all(log == logs[0] for log in logs) else None

        try:
            for node in ports:
                start(node)
            first = eventually(leader, 'initial leader')
            assert request(ports[first], '/entries', {'command': 'before-failure'})[0] == 200
            eventually(logs_equal, 'initial replication')
            follower = next(node for node in processes if node != first)
            status, body = request(ports[follower], '/entries', {'command': 'wrong-node'})
            assert status == 409 and body['leaderId'] == first, (status, body)
            print(f'Initial leader {first}: replication and follower HTTP 409 OK', flush=True)
            stop(first)
            second = eventually(leader, 'leader after failure')
            assert second != first
            assert request(ports[second], '/entries', {'command': 'after-failure'})[0] == 200
            eventually(logs_equal, 'replication after failure')
            print(f'New leader {second}: failover and committed write OK', flush=True)
            start(first)
            eventually(leader, 'restarted follower discovers leader')
            log = eventually(logs_equal, 'restarted node catches up')
            commands = [entry['command'] for entry in log['entries'] if entry['command'] is not None]
            assert commands == ['before-failure', 'after-failure'], commands
            assert request(ports[first], '/status')[1]['role'] == 'FOLLOWER'
            print('Restarted node: follower role and complete log recovery OK', flush=True)
            for node in list(processes):
                if node != second:
                    stop(node)
            status, body = request(ports[second], '/entries', {'command': 'no-majority'})
            assert status == 503, (status, body)
            log = request(ports[second], '/log')[1]
            assert log['entries'][-1]['index'] > log['commitIndex']
            print('Lost majority: HTTP 503 and uncommitted entry OK', flush=True)
        except BaseException:
            for path in Path(directory).glob('*.log'):
                print(f'\n--- {path.name} ---\n{path.read_text()[-5000:]}')
            raise
        finally:
            for node in list(processes):
                stop(node)
            for output in files:
                output.close()


if __name__ == '__main__':
    main()
