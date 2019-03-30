import threading
import time
import datetime
import random
import sys

#Samples some variables and prints them every nth second.
PRINT_STATS_INTERVAL = 60

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
#   tp = threadPool()   # instantiate the threadpool
#   workList = []       # list of works. add how many you want. Each job will be put on a unique thread, executed in parrallell
#   job1 = {}           # Create a job to the work list. Add key words: callback and callbackArgs. Your callback function!
#   job1["callback"] = myCallback  ## Your function that the thread pool shall call
#   job1["callbackArgs"] = {"arg1" : counter}  ## The arguments the thread pool will pass to myCallback()
#   job1["completedCallback"] = myCompletedFunction   ## OPTIONAL, if you wantto get a callback when a job completes (returnValue, job)
#   job1["returnedValue"] = ""   ## OPTIONAL, after a thread completes the return value will be stored here. CANNOT be combined with async call!
#   workList.append(job1)    # Append the job. You should append more than one to utalize the pool properly ;-)
#   tp.runMultiThreadJob(workList)   # run the batch job from your work list.
#                                    # Note! You can use multiple threads to call runMultiThreadJob simultaneously!
#                                    # Your thread will be blocked until all jobs have completed according to your worklist,
#                                    # unless you set sync=False, then you will get returned immediately, but can still get
#                                    # optinally notified by the provide 'completeCallback' callback
#
# ##############################################################
class threadPool():
    # ##############################################################
    # Constructor
    # maxThreads = the maximum possible threads that this pool may use
    # maxQueueLength = the max job queue length to allow in case of all threads are busy, and you are running
    # in sync=False mode. If queue is full, you will be blocked even in async mode.
    # versbose = True means some stats will be printed to stdout every minute
    # ##############################################################
    def __init__(self, maxThreads=22, maxQueueLength = 1, verbose=False):

        self.verbose = verbose
        self.lastStatPrintTime = time.time()
        self.maxMeasuredWaitList = 0
        self.abortAll = False
        self.maxThreads = maxThreads
        self.maxQueueLength = maxQueueLength
        self.globalLock = threading.Lock()
        self.threadList = []
        self.threadFreeList = []
        self.globalJobList = []

    # ##############################################################
    # call this function to execute your jobList in parrallell.
    # Function will block until all jobs are complete.
    # arguments:
    # joblist = list of dictionarys with required following keys:
    #    joblist = [{"callback" : yourCallbackFunction, "callbackArgs" : TheArgsToPassToCallback}]
    # sync = if to run synchronous or async, however, if there are no available threads even with
    #    sync==False will block until all your jobs are complete, if maxQueueLength is reached
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
            if sync is False and "returnedValue" in entry:
                print(f"{datetime.datetime.now().isoformat()} You cannot provide returnedValue field in async mode. Use \'completedCallback' instead!!")
                continue

            entry["__jobCompleteEvent__"] = threading.Event()

            with self.globalLock:
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
            #print(f"{datetime.datetime.now().isoformat()} Thread pool reach max limit of {self.maxThreads} threads")
            return False

        threadToUse["startWorkEvent"].set()
        return True

    # ##############################################################
    # Call this function to terminate all resources before de-referncing this object
    # ##############################################################
    def terminate(self, async=False):

        print(f"{datetime.datetime.now().isoformat()} Terminate thread pool")

        self.abortAll = True

        for nextThread in self.threadList:
            nextThread['terminate'] = True

        for nextThread in self.threadList:
            nextThread["startWorkEvent"].set()

        if not async:
            for nextThread in self.threadList:
                nextThread['threadHandle'].join()

            print(f"{datetime.datetime.now().isoformat()} All threads in thread pool terminated")



    # ##############################################################
    # NOT THREAD SAFE. YOU MUST LOCK ALL GLOBAL RESOURCES
    # Private function that creates a new thread with threadInfo that
    # will follow this thread forever
    # ##############################################################
    def _createNewThread(self, threadId):

        print(f"{datetime.datetime.now().isoformat()} Creating new thread {threadId}")

        threadInfo = {}
        threadInfo["threadHandle"] = threading.Thread(target=self._threadDayCare, args=(threadInfo,))
        threadInfo["threadId"] = threadId
        threadInfo["globalJobList"] = self.globalJobList
        threadInfo["threadFreeList"] = self.threadFreeList
        threadInfo["lock"] =  threading.Lock()
        threadInfo["globalLock"] = self.globalLock
        threadInfo["startWorkEvent"] = threading.Event()
        threadInfo["terminate"] = False

        self.threadList.append(threadInfo)
        self.threadFreeList.append(threadInfo)

        threadInfo["threadHandle"].start()

    # ##############################################################
    # All threads hang out here, waiting for something to do..
    # ##############################################################
    def _threadDayCare(self, threadInfo):

        while not threadInfo['terminate']:
            threadInfo["startWorkEvent"].wait() # wait until someone calls .set
            threadInfo["startWorkEvent"].clear() # next time this event will be blocked

            if threadInfo['terminate']:
                break

            while True:
                nextJob = self._takeNextFreeJob(threadInfo["globalJobList"], threadInfo["globalLock"])

                if nextJob is None:
                    break

                # Do the work
                try:
                    userReturnValue = nextJob["callback"](nextJob["callbackArgs"])
                except Exception as ex:
                    print(f"{datetime.datetime.now().isoformat()} Exception in user callback function: {ex}")
                    userReturnValue = ex

                # Report return code back to user..
                try:
                    if "completedCallback" in nextJob:
                        nextJob["completedCallback"](userReturnValue, nextJob)

                    nextJob["returnedValue"] = userReturnValue
                except Exception as ex:
                    print(f"Exception when posting return value back to user... {ex}")
                finally:
                    nextJob["__jobCompleteEvent__"].set()

            self._threadCompleatedJob(threadInfo)
            sys.stdout.flush()

        print(f"{datetime.datetime.now().isoformat()} Thread in thread pool terminated")

    # ##############################################################
    # a thread that has nothing more to do calls this function
    # ##############################################################
    def _threadCompleatedJob(self, threadInfo):
        with threadInfo["globalLock"]:
                threadInfo["threadFreeList"].append(threadInfo)

    # ##############################################################
    # returns the next job to execute from the jobList
    # ##############################################################
    def _takeNextFreeJob(self, jobList, globalLock):

        with globalLock:
            if len(jobList) > 0:
                retJob = jobList[0]
                del jobList[0]
                return retJob

        return None

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
# Test callback functions
# ##############################################################
terminate = False

## Will be called by a thread in the thread pool
def myCallback(args):
    print(f"{datetime.datetime.now().isoformat()} CALLBACK CALLED!!" + str(args))
    time.sleep(random.randint(0,10) / 10)
    if random.randint(0,5) is 0:
        raise ValueError("Error of type 9876542")
    return random.randint(0, 1000)

## Will be called by the thread pool when a callback has completed..
def jobCompletedCallback(returnValue, callbackArgs):
    print(f"Returned value from threadded function: {returnValue}, {callbackArgs}")


def threadFunc(args):
    while True:
        workList = []
        counter = 0
        jobsize = random.randint(0,4)
        sync = random.randint(0,1)

        for a in range(jobsize):
            if terminate:
                return

            job = {}
            job["callback"] = myCallback
            job["callbackArgs"] = {"arg1" : counter}
            job["completedCallback"] = jobCompletedCallback

            if sync:
                job["returnedValue"] = ""

            counter += 1
            workList.append(job)

        retval = tp.runMultiThreadJob(workList,sync)
        print(f"{datetime.datetime.now().isoformat()} {args} Done! retval: {retval} sync: {sync}")

if __name__ == "__main__":

    # Case 1
    terminate = False

    tp = threadPool(maxThreads=4, maxQueueLength=10)
    t1 = threading.Thread(target=threadFunc, args=("CALLER 1",))
    t2 = threading.Thread(target=threadFunc, args=("CALLER 2",))
    t3 = threading.Thread(target=threadFunc, args=("CALLER 3",))
    t1.start()
    t2.start()
    t3.start()

    time.sleep(10)
    print(f"{datetime.datetime.now().isoformat()} STOPPING LAUNCHING JOBS!")
    terminate = True
    time.sleep(2)
    print(f"{datetime.datetime.now().isoformat()} TERMINATING POOL")
    tp.terminate()

    t1.join()
    t2.join()
    t3.join()



