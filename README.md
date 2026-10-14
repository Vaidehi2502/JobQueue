# JobQueue
Multithreaded background system for asynchronous processing of jobs, implemented using C++20, SQLite and CMake. System receives jobs asynchronously, processes them using configurable worker pool, monitors state of jobs, provides for retries of jobs and resume of pending jobs upon restart of the application.
