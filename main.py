import sys
from operator import add, mul
from pyspark import SparkContext
import time

import matplotlib.pyplot as plt

#### Driver program

# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")

def csv_split(line):
	return line.split(',')

merge_two_lists = lambda t1, t2: [i+j for i, j in zip(t1, t2)]

# =======================================================
# number of task per job
# =======================================================

entries = sc.textFile("./data/task-events.csv").map(csv_split)
JOBID = 2

task_per_job = entries.groupBy(
	lambda task: task[JOBID]
).map(
	lambda job_task: (len(job_task[1]), 1)
).reduceByKey(add)

# print(f'on average, jobs have {round(task_per_job.mean(), 1)} tasks')

# plt.plot(*zip(*task_per_job.collect()), 'o')
# plt.xscale('log')
# plt.ylabel('Number of jobs')
# plt.xlabel('Number of tasks')
# plt.show()

# =======================================================
# computational power loss because of machine being down
# =======================================================

entries = sc.textFile("./data/machine-events.csv").map(csv_split)

ADD, REMOVE, UPDATE = 0, 1, 2
TIME, MACHINEID, EVENTTYPE, PLATID, CPUS, MEM = [i for i in range(6)]

maxtime = entries.map(lambda event: int(event[TIME])).max()

time_lost = entries.filter(
	# we remove the firsts ADD events (<=> happening at 0)
	# and the UPDATE events
	lambda event: int(event[0]) != 0 and int(event[EVENTTYPE]) != UPDATE
).map(
	# (machine ID, (eventtime, eventtype))
	lambda event: (
		event[MACHINEID],
		(int(event[TIME]), int(event[EVENTTYPE]))
	)
).aggregateByKey(
	# we sum the timestamps
	# positively if its an ADD, else negatively
	0,
	lambda acc, event: acc + {
		ADD: event[TIME],
		REMOVE: -event[TIME]
	}[event[MACHINEID]],
	add
).map(
	# if the sum is negative that means the machine is REMOVED at 
	# the end of the data. In that case we add an artificial
	# add event at maxtime
	lambda total: (total[0], maxtime + total[1] if total[1] < 0 else total[1])
)

percentage_lost = time_lost.map(
	# we change the sum to a percentage of maxtime
	lambda total: 100*total[1]/maxtime
)

# print(f'On average, each machine is down for\
#  {percentage_lost.mean()}% of the time')

# =======================================================
# repartition of machines according to CPU capacity
# =======================================================

entries = sc.textFile("./data/machine-events.csv").map(csv_split)

ADD, REMOVE, UPDATE = 0, 1, 2
TIME, MACHINEID, EVENTTYPE, PLATID, CPUS, MEM = [i for i in range(6)]
NDIVISIONS = 10

cpus = entries.filter(
	# we only keep ADD event at the begining
	# this ensure that every machine is taken once
	lambda event: int(event[EVENTTYPE]) == ADD and int(event[TIME]) == 0
).map(
	# we only keep the cpu number
	lambda event: float(event[CPUS])
).histogram([i/NDIVISIONS for i in range(NDIVISIONS + 1)])

# print('{:^{size}}'.format('Number of machine for each fraction of the max CPU', size=NDIVISIONS*7-1))
# print('-'.join([6*'-' for _ in range(NDIVISIONS)]))
# print('|'.join(['{:^6}'.format(f'<{i}') for i in cpus[0][1:]]))
# print('+'.join([6*'-' for _ in range(NDIVISIONS)]))
# print('|'.join(['{:^6}'.format(i) for i in cpus[1]]))

# print(cpus)

# bins = [(i + j)/2 for i, j in zip(cpus[0][:-1], cpus[0][1:])]

# plt.bar(bins, cpus[1], width=0.1)
# plt.title('Repartition of machines according to their CPU capacities')
# plt.ylabel('Number of machines')
# plt.xlabel('CPU capacity')
# plt.xticks(cpus[0])
# plt.show()

# =======================================================
# probabilty of eviction according to priority
# =======================================================

from operator import add

entries = sc.textFile("./data/task-events.csv").map(csv_split)
PRORITY, EVENTTYPE = 8, 5
SUBMIT, EVICT = 0, 2

submits_by_key = entries.filter(
	lambda event: int(event[EVENTTYPE]) == SUBMIT
	# We only keep the SUBMIT events
).map(
	# we change the key to the priority for the grouping
	lambda event: (int(event[PRORITY]), 1)
).reduceByKey(
	 # we reduce by key using the add operator
	add
)

evictions_by_key = entries.filter(
	# We only keep the EVICT events
	lambda event: int(event[EVENTTYPE]) == EVICT 
).map(
	# we change the key to the priority for the grouping
	lambda event: (int(event[PRORITY]), 1)
).reduceByKey(
	 # we reduce by key using the add operator
	add
)

# non parallel work because the two lists are now very small
# ~ 10 elements

evictions = {
	priority: evictions
	for priority, evictions in evictions_by_key.collect()
}

evictions_pr = sorted(
	# list of priority and percentage of eviction per submit 
	# 200% => on average, task of this priority get evicted 2 times
	[
		(priority, round(100*evictions.get(priority, 0) / submit, 2))
		for priority, submit in submits_by_key.collect()
	],
	# sorted by priority
	key=lambda x: x[0]
)

NDIVISIONS = len(evictions_pr)

# plt.plot(*zip(*evictions_pr), 'o')
# plt.ylabel('Percentage of task eviction')
# plt.xlabel('Job priority')
# plt.show()

# print('\n{:^{size}}'.format('Probability of a task being evicted for each priority', size=NDIVISIONS*7-1))
# print('-'.join([6*'-' for _ in range(NDIVISIONS)]))
# print('|'.join(['{:^6}'.format(priority) for priority, _ in evictions_pr]))
# print('+'.join([6*'-' for _ in range(NDIVISIONS)]))
# print('|'.join(['{:^6}'.format(pr) for _, pr in evictions_pr]))

# =======================================================
# repartition of job tasks on machines
# =======================================================

entries = sc.textFile("./data/task-events.csv").map(csv_split)

JOBID, MACHINEID, EVENTTYPE = 2, 4, 5
SUBMIT = 0

nb_of_jobs_per_nb_of_machines = entries.filter(
	# we only keep SUBMIT events that have non null jobid and machineid
	lambda event: (
		int(event[EVENTTYPE]) == SUBMIT and
		event[JOBID] and event[MACHINEID]
	)
).map(
	# we change the data to (jobid, machineid)
	lambda event: (int(event[JOBID]), int(event[MACHINEID]))
).groupByKey(
	# we group the set by jobid
).map(
	# we get the number of distinct machine for each job
	lambda job: (len(set(job[1])), 1)
).reduceByKey(
	add
)

# number of jobs using 1 machine
one_machine_jobs = nb_of_jobs_per_nb_of_machines.collectAsMap().get(1, 0)

total_jobs = nb_of_jobs_per_nb_of_machines.values().sum()
total_machine = nb_of_jobs_per_nb_of_machines.map(lambda e: e[0]*e[1]).reduce(add)

# print(f'{one_machine_jobs} jobs are using only 1 machine')
# print(f'{total_jobs - one_machine_jobs} are using more than 1 machine')
# print(f'On average, a job uses {round(total_machine/total_jobs, 1)} machines')

# plt.plot(*zip(*nb_of_jobs_per_nb_of_machines.collect()), 'o')
# plt.xscale('log')
# plt.title('Number of jobs running on x machines')
# plt.ylabel('Number of jobs')
# plt.xlabel('Number of machines')
# plt.show()

# =======================================================
# request more ressource = consume more ressources ?
# =======================================================

tasks = sc.textFile("./data/task-events-short.csv").map(csv_split)
usage = sc.textFile("./data/task-usage-short.csv").map(csv_split)

EVENTTYPE, CPU_REQ, MEM_REQ, DISK_REQ = 5, 9, 10, 11
SUBMIT, UPDATE_RUNNING = 0, 8

MAXMEM_USAGE, DISK_USAGE, CPU_USAGE = 10, 12, 19
JOBID, TASKINDEX = 2, 3

request = tasks.filter(
	# We filter to only keep :
	# - submit and update running events
	# - events where the cpu, mem, and disk request are non null
	lambda task: 
		int(task[EVENTTYPE]) in (SUBMIT, UPDATE_RUNNING) and
		task[CPU_REQ] and task[MEM_REQ] and task[DISK_REQ]
).map(
	# we map the list into (JOBID, (ressources requests))
	lambda task: (
		(int(task[JOBID]), int(task[TASKINDEX])),
		(task[MEM_REQ], task[CPU_REQ], task[DISK_REQ])
	)
)

usage = usage.map(
	# we map the usage table the same way as we did on the event,
	# replacing ressources requests by ressources usages
	lambda task: (
		(int(task[JOBID]), int(task[TASKINDEX])),
		(task[MAXMEM_USAGE], task[CPU_USAGE], task[DISK_USAGE])
	)
)

TASK, REQUEST, USAGE = 1, 0, 1
MEM, CPU, DISK = 0, 1, 2

# we join the two tables by JobID
usage_over_requested = request.join(usage)

NDIVISIONS = 1000

def ressources_ratio(ressources, ress_type):
	""" select a ressource type from ressources """
	return ressources.map(
		lambda task: (
			float(task[TASK][REQUEST][ress_type]),
			float(task[TASK][USAGE][ress_type])
		)
	)

# We will round the request to merge them in intervals

def ressources_intervals(ressources):
	""" transform a list of usage over request into intervals """
	return ressources.map(
		lambda ress: (round(NDIVISIONS*ress[REQUEST]), ress[USAGE])
	).aggregateByKey(
		# we now compute the average in each interval
		# (sum, count)
		(0, 0),
		# (sum, count) = (sum + current, count + 1)
		lambda acc, curr: (acc[0] + curr, acc[1] + 1),
		# merge (sum, count) = (sum1 + sum2, count1 + count2)
		lambda a, b: (a[0] + b[0], a[1] + b[1])
	).map(
		# we divide the sum by the count to have the average
		lambda ress: (ress[REQUEST], ress[USAGE][0]/ress[USAGE][1])
	)

def ressources_plot(intervals, ress_name):
	""" plot a graph of ressource usage over ressource requested """
	plt.plot(*zip(*intervals.collect()), 'o')
	# plt.xscale('log')
	plt.title(f'{ress_name} used as a function of {ress_name} requested')
	plt.ylabel(f'{ress_name} used')
	plt.xlabel(f'{ress_name} requested')
	plt.show()

ressources_plot(
	ressources_intervals(ressources_ratio(usage_over_requested, MEM)),
	'Memory'
)
ressources_plot(
	ressources_intervals(ressources_ratio(usage_over_requested, CPU)),
	'CPU'
)
ressources_plot(
	ressources_intervals(ressources_ratio(usage_over_requested, DISK)),
	'Disk'
)