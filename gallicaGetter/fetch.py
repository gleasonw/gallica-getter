import asyncio
from dataclasses import dataclass
import random
import time
from typing import Any, Callable, Literal
import aiohttp
import aiohttp.client_exceptions


@dataclass
class Response:
    text: bytes
    query: Any
    elapsed_time: float

async def post_queries_concurrently(
    queries,
    session: aiohttp.ClientSession,
    initial_rpm: float = 10,
    max_attempts: int = 3,
): 
    return await fetch_queries_concurrently(
        queries=queries,
        session=session,
        initial_rpm=initial_rpm,
        max_attempts=max_attempts,
        http_method='post',
    )


async def fetch_queries_concurrently(
    queries,
    session: aiohttp.ClientSession,
    initial_rpm: float = 10,
    max_attempts: int = 3,
    http_method: Literal['post', 'get'] = 'get',
):
    """Processes API requests in parallel, throttling to stay under rate limits. (heavy copy of OpenAI cookbok model)"""
    queries = (query for query in queries)
    seconds_to_pause_after_rate_limit_error = random.randint(20, 25)
    seconds_to_sleep_each_loop = (
        0.001  # 1 ms limits max throughput to 1,000 requests per second
    )
    estimated_max_rpm = 500

    # initialize trackers
    queue_of_requests_to_retry = asyncio.Queue()
    task_id_generator = (
        task_id_generator_function()
    )  # generates integer IDs of 1, 2, 3, ...
    status_tracker = StatusTracker()
    # single instance to track a collection of variables
    next_gallica_request = None  # variable to hold the next request to call

    # initialize available capacity counts
    available_request_capacity = initial_rpm
    last_update_time = time.time()
    tasks = []

    more_queries_to_send = True

    def set_rpm_to_max():
        nonlocal initial_rpm
        initial_rpm = estimated_max_rpm

    start = time.time()
    while True:
        # get next request (if one is not already waiting for capacity)
        if next_gallica_request is None:
            if not queue_of_requests_to_retry.empty():
                next_gallica_request = queue_of_requests_to_retry.get_nowait()
            elif more_queries_to_send:
                try:
                    # get new request
                    query = next(queries)
                    next_gallica_request = APIRequest(
                        task_id=next(task_id_generator),
                        query=query,
                        attempts_left=max_attempts,
                        session=session,
                        on_success=set_rpm_to_max,
                        http_method=http_method,
                    )
                    status_tracker.num_tasks_started += 1
                    status_tracker.num_tasks_in_progress += 1
                except StopIteration:
                    more_queries_to_send = False

        # update available capacity
        current_time = time.time()
        seconds_since_update = current_time - last_update_time
        available_request_capacity = min(
            available_request_capacity + initial_rpm * seconds_since_update / 60.0,
            initial_rpm,
        )
        last_update_time = current_time

        # if enough capacity available, call API
        if next_gallica_request:
            if available_request_capacity >= 1:
                # update counters
                available_request_capacity -= 1
                next_gallica_request.attempts_left -= 1

                # call API
                tasks.append(
                    asyncio.create_task(
                        next_gallica_request.call_gallica(
                            retry_queue=queue_of_requests_to_retry,
                            status_tracker=status_tracker,
                        )
                    )
                )
                next_gallica_request = None  # reset next_request to empty

        # if all tasks are finished, break
        if status_tracker.num_tasks_in_progress == 0:
            print(f"Finished all tasks in {time.time() - start} seconds")
            print(status_tracker)
            return (task.result() for task in tasks)

        # main loop sleeps briefly so concurrent tasks can run
        await asyncio.sleep(seconds_to_sleep_each_loop)

        # if a rate limit error was hit recently, pause to cool down
        seconds_since_rate_limit_error = (
            time.time() - status_tracker.time_of_last_rate_limit_error
        )
        if seconds_since_rate_limit_error < seconds_to_pause_after_rate_limit_error:
            remaining_seconds_to_pause = (
                seconds_to_pause_after_rate_limit_error - seconds_since_rate_limit_error
            )
            print("remaining_seconds_to_pause: ", remaining_seconds_to_pause)
            estimated_max_rpm *= 0.9
            initial_rpm = 10
            await asyncio.sleep(remaining_seconds_to_pause)
            # ^e.g., if pause is 15 seconds and final limit was hit 5 seconds ago
            print(
                f"Pausing to cool down until {time.ctime(status_tracker.time_of_last_rate_limit_error + seconds_to_pause_after_rate_limit_error)}"
            )


# dataclasses


@dataclass
class StatusTracker:
    """Stores metadata about the script's progress. Only one instance is created."""

    num_tasks_started: int = 0
    num_tasks_in_progress: int = 0  # script ends when this reaches 0
    num_tasks_succeeded: int = 0
    num_tasks_failed: int = 0
    num_rate_limit_errors: int = 0
    num_api_errors: int = 0  # excluding rate limit errors, counted above
    num_other_errors: int = 0
    time_of_last_rate_limit_error: float = (
        0  # used to cool off after hitting rate limits
    )


@dataclass
class APIRequest:
    """Stores an API request's inputs, outputs, and other metadata. Contains a method to make an API call."""

    task_id: int
    query: Any
    session: aiohttp.ClientSession
    attempts_left: int
    result = []
    on_success: Callable
    http_method: Literal['get', 'post']

    async def call_gallica_once(self):
        return await self.call_gallica(
            retry_queue=asyncio.Queue(),
            status_tracker=StatusTracker(),
        )
    
    def call(self) -> Callable:
        if self.http_method == 'get':
            return lambda: self.session.get(self.query.endpoint_url, params=self.query.params)
        elif self.http_method == 'post':
            return lambda: self.session.post(self.query.endpoint_url, headers=self.query.headers, json=self.query.payload)
        else:
            raise ValueError(f'Invalid http_action: {self.call}')

    async def call_gallica(
        self,
        retry_queue: asyncio.Queue,
        status_tracker: StatusTracker,
    ):
        """Calls Gallica API and saves results."""
        error = None
        response_bytes = None
        elapsed_time = None
        # try:
        start_time = time.time()
        call_function = self.call()
        async with call_function() as response:
            print(response.url)
            elapsed_time = time.time() - start_time
            response_bytes = await response.content.read()
            if response.status != 200:
                print(response.status)
                status_tracker.num_api_errors += 1
                error = response
                if response.status == 429:
                    status_tracker.time_of_last_rate_limit_error = time.time()
                    status_tracker.num_rate_limit_errors += 1
                    status_tracker.num_api_errors -= (
                        1  # rate limit errors are counted separately
                    )
            else:
                self.on_success()

        # except (
        #     Exception
        # ) as e:  # catching naked exceptions is bad practice, but in this case we'll log & save them
        #     if type(e) == aiohttp.client_exceptions.ClientConnectorError:
        #         status_tracker.time_of_last_rate_limit_error = time.time()
        #         status_tracker.num_rate_limit_errors += 1
        #         status_tracker.num_api_errors -= (
        #             1  # rate limit errors are counted separately
        #         )
        #     status_tracker.num_other_errors += 1
        #     error = e
        if error or response_bytes is None:
            self.result.append(error)
            if self.attempts_left:
                retry_queue.put_nowait(self)
            else:
                print(
                    f"Request {self.query} failed after all attempts. Saving errors: {self.result}"
                )
                status_tracker.num_tasks_in_progress -= 1
                status_tracker.num_tasks_failed += 1
        else:
            status_tracker.num_tasks_in_progress -= 1
            status_tracker.num_tasks_succeeded += 1

            return Response(
                query=self.query,
                text=response_bytes,
                elapsed_time=elapsed_time or 0,
            )


# functions


def task_id_generator_function():
    """Generate integers 0, 1, 2, and so on."""
    task_id = 0
    while True:
        yield task_id
        task_id += 1
