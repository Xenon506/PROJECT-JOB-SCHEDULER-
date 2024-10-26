#include <iostream>
#include <vector>
#include <queue>
#include <fstream>
#include <algorithm>
#include <functional>
#include <iomanip>

// Job structure
struct Job {
    int id;
    int arrivalTime;  // Arrival time of job
    int coresRequired;  // Number of CPU cores required
    int memoryRequired;  // Memory required in GB
    int execTime;  // Execution time in hours
    bool completed;

    Job(int id, int arrival, int cores, int memory, int exec)
        : id(id), arrivalTime(arrival), coresRequired(cores),
          memoryRequired(memory), execTime(exec), completed(false) {}
};

// Worker Node structure
struct WorkerNode {
    int id;
    int totalCores = 24;  // Total cores per worker node
    int totalMemory = 64;  // Total memory (in GB) per worker node
    int availableCores = totalCores;
    int availableMemory = totalMemory;

    bool allocateJob(const Job &job) {
        if (availableCores >= job.coresRequired && availableMemory >= job.memoryRequired) {
            availableCores -= job.coresRequired;
            availableMemory -= job.memoryRequired;
            return true;
        }
        return false;
    }

    void freeResources(const Job &job) {
        availableCores += job.coresRequired;
        availableMemory += job.memoryRequired;
    }

    double getCpuUtilization() const {
        return 100.0 * (1.0 - (double)availableCores / totalCores);
    }

    double getMemoryUtilization() const {
        return 100.0 * (1.0 - (double)availableMemory / totalMemory);
    }
};

// Comparator for Smallest Job First (by gross value)
struct SmallestJobFirst {
    bool operator()(const Job &j1, const Job &j2) {
        return (j1.execTime * j1.coresRequired * j1.memoryRequired) > (j2.execTime * j2.coresRequired * j2.memoryRequired);
    }
};

// Comparator for Shortest Duration First
struct ShortestDurationFirst {
    bool operator()(const Job &j1, const Job &j2) {
        return j1.execTime > j2.execTime;
    }
};

// Scheduler class
class Scheduler {
    std::vector<WorkerNode> nodes;
    std::queue<Job> jobQueueFCFS; // FCFS Queue
    std::priority_queue<Job, std::vector<Job>, SmallestJobFirst> jobQueueSmallest; // Smallest Job First Queue
    std::priority_queue<Job, std::vector<Job>, ShortestDurationFirst> jobQueueShortest; // Shortest Duration First Queue
    std::vector<Job> pendingJobs; // Pending Jobs that couldn't be scheduled

public:
    Scheduler(int numNodes) {
        for (int i = 0; i < numNodes; ++i) {
            nodes.emplace_back(WorkerNode{i});
        }
    }

    void addJobToQueueFCFS(const Job &job) {
        jobQueueFCFS.push(job);
    }

    void addJobToQueueSmallest(const Job &job) {
        jobQueueSmallest.push(job);
    }

    void addJobToQueueShortest(const Job &job) {
        jobQueueShortest.push(job);
    }

    // Scheduling jobs from FCFS queue
    void scheduleJobsFCFS() {
        while (!jobQueueFCFS.empty()) {
            Job job = jobQueueFCFS.front();  // Make a copy of the job
            jobQueueFCFS.pop();
            if (!allocateJobToNode(job)) {
                pendingJobs.push_back(job);  // Requeue if not allocated
            }
        }
    }

    // Scheduling jobs from Smallest Job queue
    void scheduleJobsSmallest() {
        while (!jobQueueSmallest.empty()) {
            Job job = jobQueueSmallest.top();  // Make a copy of the job
            jobQueueSmallest.pop();
            if (!allocateJobToNode(job)) {
                pendingJobs.push_back(job);
            }
        }
    }

    // Scheduling jobs from Shortest Duration queue
    void scheduleJobsShortest() {
        while (!jobQueueShortest.empty()) {
            Job job = jobQueueShortest.top();  // Make a copy of the job
            jobQueueShortest.pop();
            if (!allocateJobToNode(job)) {
                pendingJobs.push_back(job);
            }
        }
    }

    bool allocateJobToNode(Job &job) {
        for (auto &node : nodes) {
            if (node.allocateJob(job)) {
                std::cout << "Job " << job.id << " allocated to Node " << node.id << "\n";
                return true;
            }
        }
        std::cout << "Job " << job.id << " could not be allocated, re-queueing.\n";
        return false;
    }

    // Generate CPU and Memory utilization and write to CSV
    void generateUtilizationReport(const std::string &filename) const {
        std::ofstream file(filename);
        file << "NodeID,CPU Utilization (%),Memory Utilization (%)\n";
        for (const auto &node : nodes) {
            file << node.id << "," << std::fixed << std::setprecision(2)
                 << node.getCpuUtilization() << "," << node.getMemoryUtilization() << "\n";
        }
        file.close();
    }
};

// Sample Job Generation
void generateSampleJobs(Scheduler &scheduler) {
    scheduler.addJobToQueueFCFS(Job(1, 1, 10, 32, 5));
    scheduler.addJobToQueueSmallest(Job(2, 2, 5, 16, 3));
    scheduler.addJobToQueueShortest(Job(3, 3, 20, 48, 2));
    scheduler.addJobToQueueFCFS(Job(4, 4, 8, 20, 6));
    scheduler.addJobToQueueSmallest(Job(5, 5, 12, 40, 1));
}

int main() {
    Scheduler scheduler(128);  // 128 Worker nodes
    generateSampleJobs(scheduler);

    // Choose scheduling strategy
    std::cout << "Scheduling using FCFS policy:\n";
    scheduler.scheduleJobsFCFS();

    std::cout << "\nScheduling using Smallest Job First policy:\n";
    scheduler.scheduleJobsSmallest();

    std::cout << "\nScheduling using Shortest Duration First policy:\n";
    scheduler.scheduleJobsShortest();

    // Generate utilization report
    scheduler.generateUtilizationReport("utilization_report.csv");
    std::cout << "\nUtilization report generated in 'utilization_report.csv'.\n";

    return 0;
}
