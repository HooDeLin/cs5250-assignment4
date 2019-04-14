'''
CS5250 Assignment 4, Scheduling policies simulator
Sample skeleton program
Input file:
    input.txt
Output files:
    FCFS.txt
    RR.txt
    SRTF.txt
    SJF.txt
'''
import sys
import heapq

input_file = 'input.txt'

class Process:
    last_scheduled_time = 0
    def __init__(self, id, arrive_time, burst_time):
        self.id = id
        self.arrive_time = arrive_time
        self.burst_time = burst_time
    #for printing purpose
    def __repr__(self):
        return ('[id %d : arrival_time %d,  burst_time %d]'%(self.id, self.arrive_time, self.burst_time))

def FCFS_scheduling(process_list):
    #store the (switching time, proccess_id) pair
    schedule = []
    current_time = 0
    waiting_time = 0
    for process in process_list:
        if(current_time < process.arrive_time):
            current_time = process.arrive_time
        schedule.append((current_time,process.id))
        waiting_time = waiting_time + (current_time - process.arrive_time)
        current_time = current_time + process.burst_time
    average_waiting_time = waiting_time/float(len(process_list))
    return schedule, average_waiting_time

#Input: process_list, time_quantum (Positive Integer)
#Output_1 : Schedule list contains pairs of (time_stamp, proccess_id) indicating the time switching to that proccess_id
#Output_2 : Average Waiting Time
def RR_scheduling(process_list, time_quantum ):
    schedule = []
    current_time = 0
    rr_queue = []
    rr_queue.append(Process(process_list[0].id, process_list[0].arrive_time, process_list[0].burst_time))
    index = 1
    waiting_time = 0
    previous_process = (None, None)
    waiting_dict = {}
    while len(rr_queue) != 0 or index < len(process_list):
        # A new process has arrived
        current_process = None
        current_time_quantum = time_quantum
        if len(rr_queue) != 0:
            current_process = rr_queue.pop(0)
            current_time_quantum = min(time_quantum, current_process.burst_time)
            current_process.burst_time -= current_time_quantum
            if (current_process.id, current_process.arrive_time) != previous_process: # There is process switching, add to schedule
                schedule.append((current_time, current_process.id))
                waiting_time += current_time - waiting_dict.get((current_process.id, current_process.arrive_time), current_process.arrive_time)
            previous_process = (current_process.id, current_process.arrive_time)
            current_time += current_time_quantum
        else:
            current_time = process_list[index].arrive_time
        # We add new process that came into the queue for the next time quantum
        while index < len(process_list) and process_list[index].arrive_time <= current_time:
            rr_queue.append(Process(process_list[index].id, process_list[index].arrive_time, process_list[index].burst_time))
            index += 1
        # If current process is not done, we add it into the end of the queue
        if current_process is not None and current_process.burst_time != 0: # The process is not done
            rr_queue.append(current_process)
            waiting_dict[previous_process] = current_time
        print(rr_queue)

    return (schedule, waiting_time/float(len(process_list)))

def SRTF_scheduling(process_list):
    schedule = []
    index = 1
    waiting_time = 0
    previous_process = (None, None)
    srtf_heap = []
    heapq.heappush(srtf_heap, (process_list[0].burst_time, process_list[0].arrive_time, process_list[0].id))
    waiting_dict = {}
    current_time = 0
    while len(srtf_heap) != 0 or index < len(process_list):
        if len(srtf_heap) == 0: # There's no more process in the heap, but there are still processes incoming in the future
            # We fast-forward into the future
            current_time = process_list[index].arrive_time
            heapq.heappush(srtf_heap, (process_list[index].burst_time, process_list[index].arrive_time, process_list[index].id))
            index += 1
            continue
        current_burst_time, current_arrive_time, current_id = heapq.heappop(srtf_heap) # This is the process that has the smallest remaining time
        if (current_id, current_arrive_time) != previous_process: # There is process switching, add to schedule
            schedule.append((current_time, current_id))
            waiting_time += current_time - waiting_dict.get((current_id, current_arrive_time), current_arrive_time)
        previous_process = (current_id, current_arrive_time)
        time_quantum = current_burst_time # We assume that we can finish this job without preempt
        if index < len(process_list) and process_list[index].arrive_time < current_time + time_quantum: # There's a new process before the current one can finish
            time_quantum = min(time_quantum, process_list[index].arrive_time - current_time)
            heapq.heappush(srtf_heap, (process_list[index].burst_time, process_list[index].arrive_time, process_list[index].id))
            index += 1
        current_burst_time -= time_quantum
        current_time += time_quantum
        if current_burst_time != 0: # The process is not finished yet
            heapq.heappush(srtf_heap, (current_burst_time, current_arrive_time, current_id))
            waiting_dict[previous_process] = current_time

    return (schedule, waiting_time/float(len(process_list)))

def SJF_scheduling(process_list, alpha):
    schedule = []
    index = 0
    waiting_time = 0
    current_time = 0
    sjf_queue = []
    history_stats = {}
    while len(sjf_queue) != 0 or index < len(process_list):
        if len(sjf_queue) == 0: # There's no more process in the queue, but there are still processes incoming in the future
            # We fast-forward into the future
            current_time = process_list[index].arrive_time
            sjf_queue.append(Process(process_list[index].id, process_list[index].arrive_time, process_list[index].burst_time))
            index += 1
            continue
        selected_process_index = None
        selected_process_guess = None
        # We find the process
        for queue_index, process in enumerate(sjf_queue):
            guess = 5
            if history_stats.get(process.id) is not None:
                previous_burst, previous_guess = history_stats.get(process.id)
                guess = alpha * previous_burst + (1 - alpha) * previous_guess
            if selected_process_index is not None:
                if guess < selected_process_guess:
                    selected_process_index = queue_index
                    selected_process_guess = guess
            else:
                selected_process_index = queue_index
                selected_process_guess = guess
        selected_process = sjf_queue.pop(selected_process_index)
        history_stats[selected_process.id] = (selected_process.burst_time, selected_process_guess)
        schedule.append((current_time, selected_process.id))
        waiting_time += current_time - selected_process.arrive_time
        current_time += selected_process.burst_time

        while index < len(process_list) and process_list[index].arrive_time < current_time: # We add the process that arrives when the current process is running
            sjf_queue.append(Process(process_list[index].id, process_list[index].arrive_time, process_list[index].burst_time))
            index += 1 
    return (schedule,waiting_time/float(len(process_list)))


def read_input():
    result = []
    with open(input_file) as f:
        for line in f:
            array = line.split()
            if (len(array)!= 3):
                print ("wrong input format")
                exit()
            result.append(Process(int(array[0]),int(array[1]),int(array[2])))
    return result
def write_output(file_name, schedule, avg_waiting_time):
    with open(file_name,'w') as f:
        for item in schedule:
            f.write(str(item) + '\n')
        f.write('average waiting time %.2f \n'%(avg_waiting_time))


def main(argv):
    process_list = read_input()
    print ("printing input ----")
    for process in process_list:
        print (process)
    print ("simulating FCFS ----")
    FCFS_schedule, FCFS_avg_waiting_time =  FCFS_scheduling(process_list)
    write_output('FCFS.txt', FCFS_schedule, FCFS_avg_waiting_time )
    print ("simulating RR ----")
    RR_schedule, RR_avg_waiting_time =  RR_scheduling(process_list,time_quantum = 2)
    write_output('RR.txt', RR_schedule, RR_avg_waiting_time )
    print ("simulating SRTF ----")
    SRTF_schedule, SRTF_avg_waiting_time =  SRTF_scheduling(process_list)
    write_output('SRTF.txt', SRTF_schedule, SRTF_avg_waiting_time )
    print ("simulating SJF ----")
    SJF_schedule, SJF_avg_waiting_time =  SJF_scheduling(process_list, alpha = 0.5)
    write_output('SJF.txt', SJF_schedule, SJF_avg_waiting_time )

if __name__ == '__main__':
    main(sys.argv[1:])

