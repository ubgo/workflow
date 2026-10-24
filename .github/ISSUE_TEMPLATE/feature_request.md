---
name: Feature request
about: A scheduling, job or workflow need the library cannot express, or an ergonomic gap
title: "[feature] "
labels: enhancement
---

**What your app needs to do**

Describe the job, schedule or workflow in terms of what it should do, not how. A concrete case is worth more than a general one.

**What you tried**

The Go you wrote, and where it stopped. If a driver refused it with `ErrUnsupported`, paste the error.

**Which drivers need it**

memory, sqlstore (which database), Hatchet, or all. A capability is declared by a driver only when the conformance suite proves it on the real backend, so saying which drivers you need narrows where it lands.

**Anything else**

How another library or engine does it, with a link, or why the workaround is worse.
