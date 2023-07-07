import csv
import typing
import time
from multiprocessing import Process, Queue


number_of_processes_to_simulate = 4

MPI_ANY_SOURCE = -1


# function used by the workers to identify if an integer is prime
def is_prime(integer):
    if integer < 2:
        result = f"{integer} is not prime"
        return result
    for i in range(2, int(integer ** 0.5) + 1):
        if integer % i == 0:
            result = f"{integer} is not prime"
            return result
    result = f"{integer} is prime"
    return result


def mpi_application(
    rank: int,
    size: int,
    send_f: typing.Callable[[typing.Any, int], None],
    recv_f: typing.Callable[[int], typing.Any]
        ):

    if rank == 0:

        # initialize the parameters
        inputs = [8, 19, 107, 2037, 10111]
        num_workers = size - 1
        num_inputs = len(inputs)
        curr_input = 0

        # creating a csv to input results
        with open('prime.csv', 'w', newline='') as file:
            writer = csv.writer(file)

            # setting finite loop to send each input to specific workers
            while curr_input < num_inputs:
                integer = inputs[curr_input]
                worker_rank = curr_input % num_workers + 1
                send_f(integer, dest=worker_rank)
                curr_input += 1

            # sending special message for workers to stop
            for worker_rank in range(1, size):
                send_f(None, dest=worker_rank)

            # receiving results of the prime function from workers
            for _ in range(num_inputs):
                result = recv_f(MPI_ANY_SOURCE)
                writer.writerow([result])

    else:
        while True:
            # receiving inputs from the coordinator
            integer = recv_f(MPI_ANY_SOURCE)

            # checking if the input is a special message
            if integer is None:
                break

            # running the prime function and returning the result
            result = is_prime(integer)
            send_f(result, dest=0)

###############################################################################
# This is the simulator code, do not adjust


def _run_app(process_rank, size, app_f, send_queues):
    send_f = _generate_send_f(process_rank, send_queues)
    recv_f = _generate_recv_f(process_rank, send_queues)

    app_f(process_rank, size, send_f, recv_f)


def _generate_recv_f(process_rank, send_queues):

    def recv_f(from_source: int):
        while send_queues[process_rank].empty():
            time.sleep(1)
        return send_queues[process_rank].get()[1]
    return recv_f


def _generate_send_f(process_rank, send_queues):

    def send_F(data, dest):
        send_queues[dest].put((process_rank, data))
    return send_F


def _simulate_mpi(n: int, app_f):

    send_queues = {}

    for process_rank in range(n):
        send_queues[process_rank] = Queue()

    ps = []
    for process_rank in range(n):

        p = Process(
            target=_run_app,
            args=(
                process_rank,
                n,
                app_f,
                send_queues
            )
        )
        p.start()
        ps.append(p)

    for p in ps:
        p.join()
###############################################################################


if __name__ == "__main__":
    _simulate_mpi(number_of_processes_to_simulate, mpi_application)
