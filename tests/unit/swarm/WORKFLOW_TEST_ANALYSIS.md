# Swarm Workflow Functions - Test Analysis

## Overview
This document analyzes the three swarm workflow functions that handle swarm lifecycle events:
1. `swarm_start_tasks` - Triggered when swarm starts
2. `swarm_item_done` - Triggered when a swarm item completes successfully
3. `swarm_item_failed` - Triggered when a swarm item fails

## 1. swarm_start_tasks Analysis

### Purpose
Initializes the swarm by starting the first batch of tasks based on `max_concurrency`.

### Critical Operations
1. Extract swarm_task_id from context
2. Get SwarmTaskSignature from Redis
3. Check if swarm already started (idempotency)
4. Slice tasks based on max_concurrency
5. Update tasks_left_to_run in Redis
6. Fetch all TaskSignatures for initial batch
7. Start all initial tasks

### Edge Cases & Why They Matter

#### EC1: Swarm Already Started (has_swarm_started = True)
**Why it happens**: Duplicate workflow triggers, retry mechanisms, race conditions
**What should happen**: Early return without re-starting tasks
**Why it matters**: Prevents duplicate task execution, wasted resources, incorrect state
**Test**: Verify early return and no tasks started

#### EC2: max_concurrency = 0
**Why it happens**: Configuration error, dynamic adjustment
**What should happen**: No tasks start, all go to tasks_left_to_run
**Why it matters**: System hangs if no tasks can start
**Test**: Verify 0 tasks start, all queued

#### EC3: max_concurrency > len(tasks)
**Why it happens**: Small task list, dynamic scaling
**What should happen**: All tasks start, tasks_left_to_run is empty
**Why it matters**: Should work correctly, not crash with index errors
**Test**: Verify all tasks start

#### EC4: Empty tasks list
**Why it happens**: Edge case in swarm creation, cleanup race condition
**What should happen**: No errors, graceful handling
**Why it matters**: Should not crash the system
**Test**: Verify no exceptions thrown

#### EC5: Missing SwarmTaskSignature in Redis
**Why it happens**: Premature cleanup, Redis eviction, corruption
**What should happen**: get_safe raises error or returns None
**Why it matters**: Cannot proceed without swarm state
**Test**: Verify appropriate error handling

#### EC6: Missing SWARM_TASK_ID_PARAM_NAME in context
**Why it happens**: Workflow misconfiguration, context corruption
**What should happen**: KeyError raised
**Why it matters**: Critical parameter for operation
**Test**: Verify KeyError with clear message

#### EC7: TaskSignature.get_safe fails for some tasks
**Why it happens**: Task deleted between swarm creation and start, corruption
**What should happen**: asyncio.gather may raise or return None
**Why it matters**: Cannot start non-existent tasks
**Test**: Verify error handling

#### EC8: Pipeline context manager fails
**Why it happens**: Redis connection issues, transaction conflicts
**What should happen**: Exception raised, caught by outer try/except
**Why it matters**: State update critical for operation
**Test**: Verify exception propagation and logging

#### EC9: task.aio_run_no_wait fails for some tasks
**Why it happens**: Hatchet connection issues, task configuration errors
**What should happen**: asyncio.gather raises exception
**Why it matters**: Some tasks may not start
**Test**: Verify partial failure handling

---

## 2. swarm_item_done Analysis

### Purpose
Handles completion of a single swarm item - saves results, starts next task if available, completes swarm if all done.

### Critical Operations
1. Extract task IDs from context
2. Get SwarmTaskSignature
3. Acquire lock (critical for concurrent operations)
4. Append to finished_tasks
5. Append to tasks_results
6. Call handle_finish_tasks (decrement count, start next, check if done)
7. Cleanup task in finally block

### Edge Cases & Why They Matter

#### EC1: Missing SWARM_TASK_ID_PARAM_NAME
**Why it happens**: Context corruption, workflow misconfiguration
**What should happen**: KeyError in try block, task cleanup in finally
**Why it matters**: Cannot locate swarm without this
**Test**: Verify error raised, cleanup still executes

#### EC2: Missing SWARM_ITEM_TASK_ID_PARAM_NAME
**Why it happens**: Context corruption
**What should happen**: KeyError in try block
**Why it matters**: Cannot identify which item completed
**Test**: Verify error raised

#### EC3: SwarmTaskSignature not found
**Why it happens**: Swarm deleted while items running, cleanup race
**What should happen**: get_safe returns None or raises
**Why it matters**: Cannot update non-existent swarm
**Test**: Verify appropriate handling

#### EC4: Last item completing (swarm done)
**Why it happens**: Normal completion
**What should happen**: activate_success called, swarm removed
**Why it matters**: Critical success path
**Test**: Verify activate_success called, swarm cleaned up

#### EC5: Multiple items completing concurrently
**Why it happens**: Parallel execution
**What should happen**: Lock prevents race conditions in state updates
**Why it matters**: Prevents corrupted state (e.g., running_tasks going negative)
**Test**: Verify lock prevents races, all completions processed correctly

#### EC6: Exception during finished_tasks.aappend
**Why it happens**: Redis connection issues, list corruption
**What should happen**: Exception raised, caught
**Why it matters**: Task marked as complete but not recorded
**Test**: Verify error propagation, state consistency

#### EC7: Exception during tasks_results.aappend
**Why it happens**: Redis issues, serialization errors
**What should happen**: Exception raised
**Why it matters**: Result lost but task marked complete
**Test**: Verify handling

#### EC8: Exception during handle_finish_tasks
**Why it happens**: Various reasons (see handle_finish_tasks section)
**What should happen**: Exception propagated up
**Why it matters**: May prevent swarm completion or next task start
**Test**: Verify error propagation

#### EC9: Task cleanup fails in finally
**Why it happens**: Task already deleted, Redis issues
**What should happen**: try_remove should not raise
**Why it matters**: Finally block should not raise exceptions
**Test**: Verify finally executes despite errors

#### EC10: Results contain non-serializable data
**Why it happens**: Task returns complex objects
**What should happen**: Serialization error during aappend
**Why it matters**: Cannot store results
**Test**: Verify error handling

---

## 3. swarm_item_failed Analysis

### Purpose
Handles failure of a single swarm item - records failure, checks if swarm should stop, starts next task or terminates swarm.

### Critical Operations
1. Extract task IDs from context
2. Get SwarmTaskSignature
3. Acquire lock
4. Add to failed_tasks
5. Check stop_after_n_failures logic
6. Either: stop swarm (change status, activate_error, remove) OR continue (handle_finish_tasks)
7. Cleanup task in finally

### Edge Cases & Why They Matter

#### EC1: stop_after_n_failures = None (unlimited failures)
**Why it happens**: Configuration choice for fault-tolerant swarms
**What should happen**: Never stop, always continue with next task
**Why it matters**: Swarm continues despite failures
**Test**: Verify swarm continues even with many failures

#### EC2: stop_after_n_failures = 0
**Why it happens**: Configuration for fail-fast behavior
**What should happen**: Stop immediately on first failure
**Why it matters**: Bug in code - line 80: `stop_after_n_failures or 0` means 0 becomes 0, then `len >= 0` is always True
**Test**: Verify immediate stop (this may reveal a bug!)

#### EC3: stop_after_n_failures = 1 (stop on first failure)
**Why it happens**: Fail-fast swarms
**What should happen**: First failure stops swarm
**Why it matters**: Common use case
**Test**: Verify stops on first failure

#### EC4: Exactly at failure threshold
**Why it happens**: Normal operation
**What should happen**: Swarm stops, activate_error called
**Why it matters**: Boundary condition
**Test**: Verify stops at threshold, not before

#### EC5: Below failure threshold
**Why it happens**: Normal operation
**What should happen**: Continue with handle_finish_tasks
**Why it matters**: Should not stop prematurely
**Test**: Verify swarm continues

#### EC6: Multiple concurrent failures
**Why it happens**: Parallel execution
**What should happen**: Lock prevents races, correct count maintained
**Why it matters**: Could stop too early or too late
**Test**: Verify lock prevents counting errors

#### EC7: Exception during add_to_failed_tasks
**Why it happens**: Redis issues
**What should happen**: Exception raised
**Why it matters**: Failure not recorded, may not reach threshold
**Test**: Verify error handling

#### EC8: Exception during change_status
**Why it happens**: State transition error, corruption
**What should happen**: Exception raised
**Why it matters**: Swarm not marked as canceled
**Test**: Verify error handling

#### EC9: Exception during activate_error
**Why it happens**: Error callbacks fail, Hatchet issues
**What should happen**: Exception raised
**Why it matters**: Error handlers not notified
**Test**: Verify error propagation

#### EC10: Exception during remove
**Why it happens**: Cleanup issues, tasks still running
**What should happen**: Exception raised
**Why it matters**: Resources not cleaned up
**Test**: Verify error handling

#### EC11: Swarm already canceled
**Why it happens**: Multiple failures trigger cancellation simultaneously
**What should happen**: Subsequent failures find swarm already canceled
**Why it matters**: Should handle gracefully, not error
**Test**: Verify idempotency

---

## 4. handle_finish_tasks Analysis

### Purpose
Common logic for both done and failed - decrements running count, starts next task if available, checks if swarm complete.

### Critical Operations
1. Decrease running_tasks_count
2. Fill running tasks (start next from queue)
3. Check if swarm done
4. If done, activate_success

### Edge Cases & Why They Matter

#### EC1: No tasks left to run (fill_running_tasks returns 0)
**Why it happens**: All tasks started or no more queued
**What should happen**: Log "no new task", continue to done check
**Why it matters**: Normal completion path
**Test**: Verify correct logging, done check still runs

#### EC2: Swarm is done (all tasks complete)
**Why it happens**: Last task finished
**What should happen**: activate_success called, swarm removed
**Why it matters**: Critical completion path
**Test**: Verify activate_success called exactly once

#### EC3: Swarm not done (more tasks running)
**Why it happens**: Normal operation
**What should happen**: No activate_success, just decrement and fill
**Why it matters**: Premature completion would be wrong
**Test**: Verify activate_success NOT called

#### EC4: Exception during decrease_running_tasks_count
**Why it happens**: Redis issues
**What should happen**: Exception raised
**Why it matters**: Count becomes incorrect
**Test**: Verify error propagation

#### EC5: Exception during fill_running_tasks
**Why it happens**: Task retrieval fails, Hatchet issues
**What should happen**: Exception raised
**Why it matters**: Next tasks don't start
**Test**: Verify error handling

#### EC6: Exception during activate_success
**Why it happens**: Success callbacks fail
**What should happen**: Exception raised
**Why it matters**: Success not properly notified
**Test**: Verify error propagation

#### EC7: current_running_tasks goes negative
**Why it happens**: Race condition, double-decrement
**What should happen**: Should not happen with proper locking
**Why it matters**: State corruption
**Test**: Verify cannot go negative with concurrent operations

#### EC8: is_swarm_done race condition
**Why it happens**: Multiple items finish simultaneously
**What should happen**: Only one calls activate_success
**Why it matters**: Duplicate success notifications, double cleanup
**Test**: Verify activate_success called once even with concurrent finishes

---

## Testing Strategy

### 1. Sanity Tests (Happy Path)
- Basic functionality works as expected
- Most common use cases
- Verify core workflow

### 2. Edge Case Tests (Error Conditions)
- Boundary conditions
- Error handling
- Race conditions
- State consistency
- Idempotency

### 3. Integration Considerations
- These are unit tests, mock external dependencies
- Focus on logic within each function
- Test state transitions and side effects
- Verify cleanup in finally blocks

### 4. Mock Strategy
- Mock Context (hatchet_sdk.Context)
- Mock TaskSignature operations (get_safe, aio_run_no_wait, try_remove)
- Mock SwarmTaskSignature operations
- Use real Redis (fakeredis) for state
- Mock Hatchet client operations

---

## Potential Bugs Identified

### Bug 1: stop_after_n_failures = 0 logic
**Location**: swarm_item_failed:80
```python
stop_after_n_failures = swarm_task.config.stop_after_n_failures or 0
```
**Issue**: If stop_after_n_failures is 0 (valid config), this becomes 0. Then line 81:
```python
too_many_errors = len(swarm_task.failed_tasks) >= stop_after_n_failures
```
Becomes `len >= 0` which is always True, stopping swarm immediately even before first task runs.

**Fix needed**: Should be:
```python
should_stop_after_failures = swarm_task.config.stop_after_n_failures is not None
stop_after_n_failures = swarm_task.config.stop_after_n_failures if should_stop_after_failures else 0
```

### Bug 2: Missing error handling for missing task in swarm_start_tasks
**Location**: swarm_start_tasks:34-35
```python
tasks_to_run = await asyncio.gather(
    *[TaskSignature.get_safe(task_id) for task_id in tasks_ids_to_run]
)
```
**Issue**: If get_safe returns None, line 37 will try to call .aio_run_no_wait on None.
**Impact**: AttributeError instead of clear error message.

---

## Test Coverage Goals

- **Line Coverage**: 95%+ of workflow functions
- **Branch Coverage**: 100% of conditional branches
- **Edge Cases**: All identified edge cases tested
- **Concurrency**: Critical race conditions tested
- **Error Paths**: All exception handling paths tested
