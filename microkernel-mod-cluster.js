/*
**  Microkernel -- Microkernel for Server Applications
**  Copyright (c) 2016-2021 Dr. Ralf S. Engelschall <rse@engelschall.com>
**
**  Permission is hereby granted, free of charge, to any person obtaining
**  a copy of this software and associated documentation files (the
**  "Software"), to deal in the Software without restriction, including
**  without limitation the rights to use, copy, modify, merge, publish,
**  distribute, sublicense, and/or sell copies of the Software, and to
**  permit persons to whom the Software is furnished to do so, subject to
**  the following conditions:
**
**  The above copyright notice and this permission notice shall be included
**  in all copies or substantial portions of the Software.
**
**  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
**  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
**  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
**  IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
**  CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
**  TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
**  SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

/*  external requirements  */
const sprintf = require("sprintfjs")
const cluster = require("cluster")

/*  the Microkernel module  */
class Module {
    get module () {
        return {
            name:  "microkernel-mod-cluster",
            tag:   "CLUSTER",
            group: "BOOT",
            after: [ "CTX", "OPTIONS", "LOGGER", "SHUTDOWN", "DAEMON" ]
        }
    }
    latch (kernel) {
        kernel.latch("options:options", (options) => {
            options.push({
                name: "cluster", type: "integer", default: 0,
                help: "Enable clustering server mode", helpArg: "INSTANCES"
            })
        })
    }
    prepare (kernel) {
        /*  we operate only if clustering is requested  */
        if (kernel.rs("options:options").cluster === 0)
            return

        /*  we operate only in non daemonized mode  */
        if (kernel.rs("options:options").daemon || kernel.rs("options:options").daemon_kill)
            return

        /*  provide cluster information  */
        kernel.rs("cluster", cluster)

        /*  update process mode  */
        const mode = cluster.isMaster ? "master" : "worker"
        kernel.rs("ctx:procmode", mode)

        if (mode === "master") {
            /*  act in MASTER role   */

            /*  log worker process events  */
            cluster.on("fork", (worker) => {
                kernel.sv("log", "cluster", "debug", sprintf("worker #%d (pid: %d): forked",
                    worker.id, worker.process.pid))
            })
            cluster.on("online", (worker) => {
                kernel.sv("log", "cluster", "trace", sprintf("worker #%d (pid: %d): online",
                    worker.id, worker.process.pid))
            })
            cluster.on("listening", (worker, addr) => {
                kernel.sv("log", "cluster", "trace", sprintf("worker #%d (pid: %d): listening to %s:%d [TCPv%d]",
                    worker.id, worker.process.pid,
                    addr.address, addr.port, addr.addressType))
            })
            cluster.on("disconnect", (worker) => {
                kernel.sv("log", "cluster", "trace", sprintf("worker #%d (pid: %d): IPC channel disconnected",
                    worker.id, worker.process.pid))
            })

            /*  react on worker process termination  */
            cluster.on("exit", (worker, code, signal) => {
                /*  log event  */
                const way = worker.suicide ? "expectedly" : "unexpectedly"
                if (signal)
                    kernel.sv("log", "cluster", "trace", sprintf("worker #%d (pid: %d): %s terminated by signal %s",
                        worker.id, worker.process.pid, way, signal))
                else if (code !== 0)
                    kernel.sv("log", "cluster", "trace", sprintf("worker #%d (pid: %d): %s terminated due to error (exit code %d)",
                        worker.id, worker.process.pid, way, code))
                else
                    kernel.sv("log", "cluster", "trace", sprintf("worker #%d (pid: %d): %s terminated with success",
                        worker.id, worker.process.pid, way))

                /*  respawn accidentally terminated worker  */
                if (!worker.suicide) {
                    kernel.sv("log", "cluster", "info", sprintf("re-forking WORKER process after unexpected termination"))
                    cluster.fork()
                }
            })

            /*  spawn requested number of workers  */
            const workers = kernel.rs("options:options").cluster
            kernel.sv("log", "cluster", "info", sprintf("forking %d WORKER processes", workers))
            for (let i = 0; i < workers; i++)
                cluster.fork()
        }
        else if (mode === "worker") {
            /*  act in WORKER role  */
            process.on("message", (msg) => {
                if (msg === "shutdown") {
                    kernel.sv("log", "cluster", "trace", sprintf("worker #%d (pid: %d): received shutdown message",
                        cluster.worker.id, cluster.worker.process.pid))
                    kernel.sv("shutdown", "received MASTER shutdown message")
                }
            })
        }

        /*  add process mode information to all logging messages  */
        kernel.latch("logger:msg", (msg) => {
            let id = "MASTER"
            if (cluster.isWorker)
                id = "WORKER-" + cluster.worker.id
            msg = "[" + id + "]: " + msg
            return msg
        })

        /*  add process mode information to process title  */
        kernel.latch("title:title", (title) => {
            if (mode === "master")
                title += " [MASTER]"
            else if (mode === "worker")
                title += " [WORKER-" + cluster.worker.id + "]"
            return title
        })
    }
    stop (kernel) {
        if (kernel.rs("ctx:procmode") !== "master")
            return

        /*  we operate only in non daemonized mode  */
        if (kernel.rs("options:options").daemon || kernel.rs("options:options").daemon_kill)
            return

        return new Promise((resolve /*, reject */) => {
            /*  send shutdown message to all workers and disconnect them  */
            kernel.sv("log", "cluster", "info", "shutdown WORKER processes")
            Object.keys(cluster.workers).forEach((id) => {
                const worker = cluster.workers[id]
                if (worker.isConnected()) {
                    worker.send("shutdown")
                    worker.disconnect()
                }
            })

            /*  await the final death of all workers  */
            const interval = setInterval(() => {
                /*  determine still active workers  */
                const workers = Object.keys(cluster.workers)
                if (workers.length === 0) {
                    /*  stop awaiting  */
                    clearTimeout(interval)
                    resolve()
                }
                else {
                    /*  kill still remaining workers  */
                    workers.forEach((id) => {
                        const worker = cluster.workers[id]
                        if (!worker.isDead()) {
                            kernel.sv("log", "cluster", "trace",
                                sprintf("remaining worker #%d (pid: %d): to be killed now",
                                    worker.id, worker.process.pid))
                            /* eslint no-empty: 0 */
                            try {
                                worker.kill()
                            }
                            catch (ex) {
                                /*  NOP  */
                            }
                        }
                    })
                }
            }, 100)
        })
    }
}

/*  export the Microkernel module  */
module.exports = Module

