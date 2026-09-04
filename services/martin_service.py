import subprocess
import os


def restart_martin():
    uid = os.getuid()  # must be the user that owns the user service
    env = os.environ.copy()
    env["XDG_RUNTIME_DIR"] = f"/run/user/{uid}"
    env["DBUS_SESSION_BUS_ADDRESS"] = f"unix:path=/run/user/{uid}/bus"

    cmd = ["systemctl", "--user", "restart", "martin.service"]

    print('cmd: ', cmd)

    try:
        out = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            env=env,              # <-- add this
        )
        print('out: ', out)
        print('out.stdout: ', out.stdout)
        return {"ok": True, "stdout": out.stdout}
    except subprocess.CalledProcessError as e:
        return {"ok": False, "stdout": e.stdout, "stderr": e.stderr, "code": e.returncode}
