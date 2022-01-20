
import threading
import multiprocessing as mp
from multiprocessing import Process, Pipe, Queue, Lock
import time
import datetime
import random
import sys

#Samples some variables and prints them every nth second.
PRINT_STATS_INTERVAL = 60


# https://docs.python.org/3.6/library/multiprocessing.html

# ##############################################################
# "Advanced" thread pool
# This is a multi-threaded thread pool, and by that I mean you
# can use multiple threads to call it.
# Example:
#  Thread 1 calls the pool with 5 jobs that will be executed in parrallel,
#  at the same time Thread 2 calls the same pool with 3 jobs that will also
#  execute at the same time, as long as there are enough free threads.
#
# Usage example:
#   tp = threadPool()   # instantiate the processpool
#   workList = []       # list of works. add how many you want. Each job will be put on a unique process, executed in parrallell
#   job1 = {}           # Create a job to the work list. Add key words: callback and callbackArgs. Your callback function!
#   job1["callback"] = myCallback
#   job1["callbackArgs"] = {"arg1" : counter}
#   job1["retData"] = None   # data returned from the myCallback function will be returned here
#   workList.append(job1)    # Append the job. You should append more than one to utalize the pool properly ;-)
#   tp.runMultiThreadJob(workList)   # run the batch job from your work list.
#                                    # Note! You can use multiple threads to call runMultiThreadJob simultaneously!
#                                    # Your thread will be blocked until all jobs have completed according to your worklist
#
# ##############################################################
class processPool():
    # ##############################################################
    # Function
    # ##############################################################
    def __init__(self, maxThreads=22, maxQueueLength = 1, verbose = False):

        self.verbose = verbose
        self.lastStatPrintTime = time.time()
        self.maxMeasuredWaitList = 0
        self.globalWorkId = 0
        self.abortAll = False
        self.maxThreads = maxThreads
        self.maxQueueLength = maxQueueLength
        self.globalLock = Lock()
        self.threadList = []
        self.threadFreeList = []
        self.globalJobList = []
        self.runningJobList = []
        self.completedWorkEvent = mp.Queue()
        self.jobCompleteCallbackThread = threading.Thread(target=self._jobCompleteCallback, args=())
        self.jobCompleteCallbackThread.start()


    # ##############################################################
    # Empty the inter-process queue of signals about that a process
    # finished a job with globalWorkId, and it was threadId process
    # that is now idle
    # ##############################################################
    def _jobCompleteCallback(self):

        while not self.abortAll:

            try:
                returnedData = self.completedWorkEvent.get()
                globalWorkId = returnedData[0]
                threadId     = returnedData[1]
                retData      = returnedData[2]

                with self.globalLock:
                    indexToDelete = None

                    for index, nextEntry in enumerate(self.runningJobList):

                        if nextEntry["__globalWorkId__"] == globalWorkId:
                            if "retData" in nextEntry:
                                nextEntry["retData"] = retData

                            nextEntry["__jobCompleteEvent__"].set()
                            indexToDelete = index
                            break

                    # remove the thread from running job list
                    if indexToDelete is not None:
                        del self.runningJobList[indexToDelete]

                    # put the thread back in the free thread list
                    for nextThread in self.threadList:
                        if nextThread["threadId"] == threadId:
                            self.threadFreeList.append(nextThread)

                # One thread finished, so if we have more jobs in the queue it is time to
                # process it now
                self._kickNextFreeThread()
            except Exception as ex:
                print(f"Exception in _jobCompleteCallback thread: {ex}")

        print("_jobCompleteCallback thread terminated")



    # ##############################################################
    # call this function to execute your jobList in parrallell.
    # Function will block until all jobs are complete.
    # arguments:
    # joblist = list of dictionarys with required following keys:
    #    joblist = [{"callback" : yourCallbackFunction, "callbackArgs" : TheArgsToPassToCallback}]
    # sync = if to run synchronous or async, however, if there are no available threads even with
    #    sync==False will block until all your jobs are complete, so there will no be a work queue built up
    #    Mayby should add flag "allowQueue" to also allow that behaviour...
    #
    # return value:
    #    The number of jobs from inputted work list that was put in a queue, e.g. there are no available
    #    threads in the pool. The job will be executed later when there are free threads, but if returned
    #    value is not 0, it means there is a queue building up....
    # ##############################################################
    def runMultiThreadJob(self, jobList, sync=True):

        if self.abortAll:
            time.sleep(1)
            return 0

        notStartedQueue = 0

        for entry in jobList:
            if "callback" not in entry:
                print(f"{datetime.datetime.now().isoformat()} You must provide a callback function for each job!!")
                continue
            if "callbackArgs" not in entry:
                print(f"{datetime.datetime.now().isoformat()} You must provide a callbackArgs for each job!!")
                continue

            with self.globalLock:
                self.globalWorkId += 1
                entry["__jobCompleteEvent__"] = threading.Event()
                entry['__globalWorkId__'] = self.globalWorkId
                self.globalJobList.append(entry)

            if not self._kickNextFreeThread():
                notStartedQueue += 1

        unprocessedJobQueue = len(self.globalJobList) - self.maxThreads
        if unprocessedJobQueue < 0:
            unprocessedJobQueue = 0

        if unprocessedJobQueue > self.maxMeasuredWaitList:
            self.maxMeasuredWaitList = unprocessedJobQueue
            print(f"{datetime.datetime.now().isoformat()} Work queueSize with jobs that was not scheduled to a thread just reached new max value: {unprocessedJobQueue} (Max allowed: {self.maxQueueLength})")

        if self.verbose:
            self.printStats()

        if sync:
            # run synchronous. Wait until all jobs are complete.

            for nextJob in jobList:
                nextJob["__jobCompleteEvent__"].wait()
        else:
            # run async! but only if the unprocessed work queue is not bigger than maxQueueLength
            if unprocessedJobQueue >= self.maxQueueLength:

                print(f"{datetime.datetime.now().isoformat()} Thread pool async call blocking as the work queue reached max length: {self.maxQueueLength}. unprocessed queueSize: {unprocessedJobQueue}")

                for nextJob in jobList:
                    nextJob["__jobCompleteEvent__"].wait()

        return len(self.globalJobList)

    # ##############################################################
    # Private function that returns a idle thread. If non exists,
    # a new thread will be created, unless if max thread count is reached.
    # Then null is returned.
    # ##############################################################
    def _getFreeThread(self):

        with self.globalLock:
            if len(self.threadFreeList) == 0:
                if len(self.threadList) >= self.maxThreads:
                    return None
                else:
                    self._createNewThread(len(self.threadList))

            threadToReturn = self.threadFreeList[0]
            del self.threadFreeList[0]
        return threadToReturn


    # ##############################################################
    # Private function that will signal next free thread to start working
    # If a thread started working, True is returned, else False
    # ##############################################################
    def _kickNextFreeThread(self):

        threadToUse = self._getFreeThread()

        if threadToUse is None:
            print(f"{datetime.datetime.now().isoformat()} Thread pool reach max limit of {self.maxThreads} threads")
            return False

        nextJob = self._takeNextFreeJob()

        if nextJob is None:
            #No work to do.. Well, return the thread to the pool
            with self.globalLock:
                self.threadFreeList.append(threadToUse)
            return False
        else:
            threadToUse["interProcessQueue"].put({'callback': nextJob['callback'], 'callbackArgs': nextJob['callbackArgs'], 'globalWorkId': nextJob['__globalWorkId__']})
            return True

    # ##############################################################
    # Call this function to terminate all resources before de-referncing this object
    # ##############################################################
    def terminate(self, sync=True):

        print(f"{datetime.datetime.now().isoformat()} Terminate thread pool")

        self.abortAll = True
        self.completedWorkEvent.put([[], []])

        with self.globalLock:

            for nextThread in self.threadList:
                nextThread["interProcessQueue"].put(None)

            for nextJob in self.runningJobList:
                nextJob["__jobCompleteEvent__"].set()

            for nextJob in self.globalJobList:
                nextJob["__jobCompleteEvent__"].set()

        if sync:
            for nextThread in self.threadList:
                nextThread['threadHandle'].join()

            self.jobCompleteCallbackThread.join()
            print(f"{datetime.datetime.now().isoformat()} All threads in thread pool terminated")

    # ##############################################################
    # NOT THREAD SAFE. YOU MUST LOCK ALL GLOBAL RESOURCES
    # Private function that creates a new thread with threadInfo that
    # will follow this thread forever
    # ##############################################################
    def _createNewThread(self, threadId):

        print(f"{datetime.datetime.now().isoformat()} Creating new process {threadId}")

        threadInfo = {}
        threadInfo["interProcessQueue"] = Queue()
        threadInfo["threadId"] = threadId
        threadInfo["threadHandle"] = Process(
            target=_threadDayCare,
            args=(
                threadInfo["interProcessQueue"],
                self.globalLock,
                self.completedWorkEvent,
                threadInfo["threadId"], ))
        threadInfo["CompletedWorkEvent"] = mp.Event()
        self.threadList.append(threadInfo)
        self.threadFreeList.append(threadInfo)

        threadInfo["threadHandle"].start()

    # ##############################################################
    # Prints some stats about the thread pool
    # ##############################################################
    def printStats(self):

        timeNow = time.time()
        diff = timeNow - self.lastStatPrintTime

        if diff > PRINT_STATS_INTERVAL:
            self.lastStatPrintTime = timeNow

            queue = len(self.globalJobList) - self.maxThreads
            if queue < 0:
                queue = 0
            print(f"{datetime.datetime.now().isoformat()} SNAPSHOT: Jobs queued {queue}, free threads {len(self.threadFreeList)}, runningThreads {len(self.threadList) - len(self.threadFreeList)} totThreads: {len(self.threadList)}, maxMeasuredWaitList {self.maxMeasuredWaitList}")


    # ##############################################################
    # returns the next job to execute from the jobList
    # ##############################################################
    def _takeNextFreeJob(self):

        with self.globalLock:
            if len(self.globalJobList) > 0:
                retJob = self.globalJobList[0]
                del self.globalJobList[0]
                self.runningJobList.append(retJob)
                return retJob

        return None

# ##############################################################
# All threads hang out here, waiting for something to do..
# ##############################################################
def _threadDayCare(queue, globalLock, completedWorkEvent, threadId):

    while True:
        sys.stdout.flush()
        dataFromPipe = queue.get()

        if dataFromPipe is None:
            print(f"{datetime.datetime.now().isoformat()} Thread in thread pool terminated")
            return

        try:
            retData = None
            retData = dataFromPipe["callback"](dataFromPipe["callbackArgs"])
        except Exception as ex:
            print(f"{datetime.datetime.now().isoformat()} Exception in user callback function: "+str(ex))
        finally:
            completedWorkEvent.put([dataFromPipe["globalWorkId"], threadId, retData])

# ##############################################################
# Test callback functions
# ##############################################################
terminate = False

def myCallback(args):
    print(f"{datetime.datetime.now().isoformat()} CALLBACK CALLED!!" + str(args))
    time.sleep(random.randint(1,5))
    return {"retData": random.randint(100, 999)}


def threadFunc(args):
    while not terminate:
        workList = []
        counter = 0
        jobsize =  random.randint(0,25)
        sync = random.randint(0,1)


        for a in range(jobsize):
            if terminate:
                break

            job = {}
            job["callback"] = myCallback
            job["callbackArgs"] = {"arg1" : counter}

            counter += 1
            workList.append(job)

        retval = tp.runMultiThreadJob(workList,sync)
        print(f"{datetime.datetime.now().isoformat()} {args} Done! retval: {retval} sync: {sync}")

    print("Terminating test thread")

if __name__ == "__main__":

    tp = processPool(maxThreads=2, maxQueueLength=10)
    workList = []
    job = {}
    job["callback"] = myCallback
    job["callbackArgs"] = {"arg1" : 3}
    job["retData"] = None
    workList.append(job)
    job = {}
    job["callback"] = myCallback
    job["callbackArgs"] = {"arg1" : 4}
    job["retData"] = None
    workList.append(job)
    retval = tp.runMultiThreadJob(workList,True)
    terminate = True
    tp.terminate()

    # Case 1
    terminate = False

    tp = processPool(maxThreads=50, maxQueueLength=5)
    t1 = threading.Thread(target=threadFunc, args=("CALLER 1",))
    t2 = threading.Thread(target=threadFunc, args=("CALLER 2",))
    t3 = threading.Thread(target=threadFunc, args=("CALLER 3",))
    t1.start()
    t2.start()
    t3.start()

    time.sleep(50)
    print(f"{datetime.datetime.now().isoformat()} STOPPING LAUNCHING JOBS!")
    terminate = True
    time.sleep(1)
    print(f"{datetime.datetime.now().isoformat()} TERMINATING POOL")
    tp.terminate()

    t1.join()
    t2.join()
    t3.join()



