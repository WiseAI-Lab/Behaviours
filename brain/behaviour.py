"""
    Control the integration between Agent and Behaviours.
"""
import asyncio
import queue
from typing import List, Dict, Tuple, Optional
import time

from wise_agent.behaviours import InternalBehaviour, Behaviour
from wise_agent.acl import ACLMessage
from wise_agent.base_types import MemoryPiece
from wise_agent.exceptions import BrainStepTimeoutError
from wise_agent.utility import logger, start_task


class BrainBehaviour(InternalBehaviour):
    def __init__(self, agent):
        """This method initializes the Behaviour class with an agent instance

            :param agent: agent instance that will execute the behaviours
            established by the protocol

        """
        super(BrainBehaviour, self).__init__(agent)
        self.timeout: float = 0.1
        self.sleep_time: Optional[int] = None

    def execute(self, message: ACLMessage):
        """
            Receive message to update self.
        """
        # Do nothing now.

    def step(self, timeout: float):
        """
            Just like a iterable step and it should not blocked.
        """
        try:
            memory_piece: Tuple[int, MemoryPiece] = self.agent.memory_handler.wait_and_get(
                self.agent.memory_pieces_queue, timeout=timeout)
            # create a task in thread or process pool.
            self._running_tasks = start_task(self._running_tasks, self._tasks_pool, self._dispatch_behaviour,
                                             memory_piece)
        except queue.Empty:
            raise BrainStepTimeoutError

    async def run(self):
        """
            It is the same as Function--On start
        """
        # If sleep_time is None that default value is timeout.
        if self.sleep_time is None:
            self.sleep_time = self.timeout
        # Start the loop event.
        start_time = int(time.time())
        while True:
            try:
                self.step(self.timeout)
            except asyncio.CancelledError:
                # TODO:Do something here when a daemon process cancelled.
                pass
            except BrainStepTimeoutError:
                # Just Record the sleep time
                point_time = int(time.time())
                record_time = point_time - start_time
                if record_time % 1800 == 0:
                    start_time = point_time
                    logger.info(f"Brain not receive anything in {record_time} second...Waiting")

                # sleep await.
                await asyncio.sleep(self.sleep_time)

    async def on_start(self, daemon_tasks: List[Behaviour]):
        """
            on_start
        """
        # Start brain event and the other behaviour.
        await asyncio.gather(
            self.run(),  # Brain Wait for 1 second.
            *[b.run() for b in daemon_tasks]
        )

    # -----------------Task--------------------------
    def _dispatch_behaviour(self, memory_piece: MemoryPiece):
        """
            Dispatch the behaviour to execute the ACLMessage
        """
        if not isinstance(memory_piece, MemoryPiece):
            raise ValueError("It should be a Tuple(int, TaskPiece) object in Queue")
        executors: List[str] = memory_piece.executors
        cur_behaviour_names: Dict[str, Behaviour] = {b.__class__.__name__: b for b in self.agent.behaviours}
        if executors is None:
            # all behaviours
            executors = list(cur_behaviour_names.keys())

        for behaviour in executors:
            # Check it whether in behaviours
            if behaviour in cur_behaviour_names.keys():
                cur_behaviour_names[behaviour].execute(memory_piece.content)
            else:
                logger.exception(f"Behaviour:{behaviour} not exist in agent.behaviours.")
