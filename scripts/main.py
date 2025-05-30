import time, threading, sys, os
import json
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import ast, numpy as np

from kortex_api.autogen.client_stubs.BaseClientRpc import BaseClient
from kortex_api.autogen.client_stubs.BaseCyclicClientRpc import BaseCyclicClient
from kortex_api.autogen.messages import Base_pb2

# ---------- const ----------
TIMEOUT_S   = 5.0
WORKERS     = ThreadPoolExecutor(max_workers=1)

# ---------- check if action ends or interrupted ----------
def check_for_end_or_abort(event):
    def _check(notif, _):
        if (notif.action_event in
            (Base_pb2.ACTION_END, Base_pb2.ACTION_ABORT)):
            event.set()
    return _check

# ---------- data collection ----------
class ActionStats:
    def __init__(self):
        self.calls = self.success = self.timeout = 0
        self.total = 0.0
    def add(self, elapsed, ok, timed_out):
        self.calls += 1
        self.total += elapsed
        self.success += ok
        self.timeout += timed_out
    @property
    def avg(self):
        return self.total / self.calls if self.calls else 0.0

stats = ActionStats()

def gripper_move(base, percent: float, timeout=3.0):
    goal = Base_pb2.GripperCommand()
    goal.mode = Base_pb2.GRIPPER_POSITION
    goal.gripper.finger.add().value = percent   # 0=open 1=close

    e = threading.Event()
    h = base.OnNotificationGripperTopic(
        lambda n, _: e.set() if n.gripper_event ==
                             Base_pb2.GRIPPER_MOVEMENT_COMPLETED else None,
        Base_pb2.NotificationOptions())

    base.SendGripperCommand(goal)
    ok = e.wait(timeout)
    base.Unsubscribe(h)
    return ok

# ---------- per action ----------
def cartesian_action_movement(base, base_cyclic, command):
    start = time.perf_counter()
    
    action = Base_pb2.Action()
    fb = base_cyclic.RefreshFeedback()
    pose = action.reach_pose.target_pose
    pose.x = fb.base.tool_pose_x + command["world_vector"][0]
    pose.y = fb.base.tool_pose_y + command["world_vector"][1]
    pose.z = fb.base.tool_pose_z + command["world_vector"][2]
    pose.theta_x = fb.base.tool_pose_theta_x + command["rotation_delta"][0]
    pose.theta_y = fb.base.tool_pose_theta_y + command["rotation_delta"][1]
    pose.theta_z = fb.base.tool_pose_theta_z + command["rotation_delta"][2]

    e = threading.Event()
    h = base.OnNotificationActionTopic(check_for_end_or_abort(e),
                                       Base_pb2.NotificationOptions())
    base.ExecuteAction(action)
    ok_arm = e.wait(TIMEOUT_S)
    base.Unsubscribe(h)

    grip_val = command["open_gripper"][0]
    ok_grip = True
    if grip_val is not None:
        ok_grip = gripper_move(base, grip_val)

    elapsed = time.perf_counter() - start
    return elapsed, (ok_arm and ok_grip)

# ---------- test loop ----------
def test(base, base_cyclic, txt="../test.txt", hz=10):
    interval = 1/hz
    t_next = time.perf_counter()

    with open(txt, encoding="utf-8") as f:
        for line in f:
            command = json.loads(line)

            fut = WORKERS.submit(cartesian_action_movement,
                                 base, base_cyclic, command)
            try:
                elapsed, ok = fut.result(timeout=TIMEOUT_S+0.2)
                stats.add(elapsed, ok, timed_out=0)
            except TimeoutError:
                stats.add(0.0, ok=0, timed_out=1)
                print("action timeout")

            t_next += interval
            time.sleep(max(0, t_next - time.perf_counter()))

    print(f"\n--- Stats ---\ncalls    {stats.calls}"
          f"\nsuccess  {stats.success}"
          f"\ntimeouts {stats.timeout}"
          f"\navg {stats.avg*1e3:.1f} ms")

# ---------- main ----------
def main() -> int:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    import utilities                                      # kinova example helper

    args = utilities.parseConnectionArguments()
    with utilities.DeviceConnection.createTcpConnection(args) as router:
        base        = BaseClient(router)
        base_cyclic = BaseCyclicClient(router)

        test(base, base_cyclic, txt="../test.jsonl", hz=10)

    return 0

if __name__ == "__main__":
    sys.exit(main())
