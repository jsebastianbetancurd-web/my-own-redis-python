# Architecture & Concepts

This document explains the "How" and "Why" behind this Redis implementation in simple terms. If you're new to backend development or data systems, this is for you!

## 1. How does it talk? (The RESP Protocol)

When you type `SET key value` in a Redis client, it doesn't just send that text. It translates it into a specific "language" called **RESP** (Redis Serialization Protocol).

Imagine you are sending a list of words. RESP formats it like this:
- `*3` (This means: "I'm sending 3 things")
- `$3\r\nSET` (This means: "The first thing is 3 characters long and it is 'SET'")
- `$3\r\nkey`
- `$5\r\nvalue`

**Why?** Computers are much faster at reading "length-prefixed" data than searching for spaces or commas. It makes the database incredibly fast.

## 2. Where does it store data? (In-Memory)

Most databases (like MySQL) store everything on your hard drive. Redis is different—it stores everything in **RAM** (the fast memory).

In this project, we use a simple **Python Dictionary** (`{}`) as our primary storage.
- **Pros**: Accessing data is near-instant.
- **Cons**: If the computer turns off, the data is gone (unless we save it... see below!).

## 3. How does it handle many people at once? (Concurrency)

If two people try to save a key at the exact same millisecond, what happens?
We use **Threads**. For every new person (client) that connects, we create a new "worker" (thread) to talk to them.

To make sure these workers don't trip over each other when writing to the dictionary, we use **Locks**. It’s like a "bathroom key"—only one worker can hold the key to the dictionary at a time.

## 4. Saving for later (Persistence)

To solve the "data is gone if the computer turns off" problem, we implemented two strategies:

### A. RDB (The Snapshot)
Every once in a while, we take a "picture" of the entire dictionary and save it as a binary file. When the server starts up again, it reads this file to restore the data.

### B. AOF (The Journal)
Every time someone writes something (like `SET`), we write that command to a text file. It's like a diary of everything that ever happened. If the server crashes, we just "replay" the diary to get back to where we were.

## 5. Growing bigger (Replication)

What if one computer isn't enough? We implemented **Master/Replica Replication**.
- The **Master** is the boss—it handles the writes.
- The **Replicas** are the helpers—they copy everything the Master does.

This allows us to have multiple copies of the data across different computers!

## 6. Complex Operations (Transactions)

Sometimes you want to do 5 things at once, and you want them to either *all* happen or *none* of them happen. This is what `MULTI` and `EXEC` do.

We also implemented **Optimistic Concurrency Control** using `WATCH`. It basically says: "I want to change this key, but only if nobody else has touched it since I last looked." If someone else changed it, we cancel the transaction to keep the data safe.
