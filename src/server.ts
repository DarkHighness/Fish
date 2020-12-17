import * as fs from "fs"
import * as Koa from "koa"
import * as Router from "koa-router"
import * as dayjs from "dayjs"
import * as koaBodyParser from "koa-bodyparser"
import {Fish, FishTask} from "./fish";
import fp = fs.promises;

interface ServerConfig {
    port: number
}

class FishServer {
    private readonly configPath = "./server.json"

    private fish : Fish
    private config : ServerConfig
    private app : Koa
    private router : Router

    initRoutes() {
        this.router.get("/", async (ctx, next) => {
            ctx.body = "Fish is running. Made With ❤"
        })

        this.router.get("/config", async (ctx, next) => {
            ctx.body = this.fish.getConfig()
        })

        this.router.get("/tasks", async (ctx, next) => {
            ctx.body = this.fish.getTasks()
        })

        this.router.get("/task", async (ctx, next) => {
            const { name } = ctx.query

            const info = await this.fish.getTaskInfo(name)

            ctx.body = {
                ...info,
                lastRun: dayjs(info.lastRun).format("YYYY-MM-DD HH:mm"),
                lastFiles: info.lastFiles.map(v => {
                    return {
                        ...v,
                        lastModified: dayjs(v.lastModified).format("YYYY-MM-DD HH:mm")
                    }
                })
            }
        })

        this.router.get("/sudoTask", async (ctx, next) => {
            const { name } = ctx.query

            try {
                await this.fish.sudoTask(name)

                ctx.body = {
                    status: "ok"
                }
            } catch (error) {
                ctx.body = {
                    status: "error",
                    error
                }
            }
        })

        this.router.get("/query", async (ctx, next) => {
            const { alias, keyword } = ctx.query

            ctx.body = await this.fish.query(alias, keyword)
        })

        this.router.get("/queryAll", async (ctx, next) => {
            const { keyword } = ctx.query

            ctx.body = await this.fish.queryInAll(keyword)
        })

        this.router.post("/addTask", async (ctx, next) => {
            const task = (ctx.request as any).body as FishTask;

            const oldTask = this.fish
                .getTasks()
                .find(v => v.name === task.name)

            if (oldTask === undefined) {
                await this.fish.addTask(task)

                ctx.body = {
                    status: "ok"
                }
            } else {
                ctx.body = {
                    status: "error"
                }
            }
        })

        this.router.get("/removeTask", async (ctx, next) => {
            const { name } = ctx.query

            await this.fish.removeTask(name)

            ctx.body = {
                status: "ok"
            }
        })
    }

    async setup() {
        this.config = JSON.parse((await fp.readFile(this.configPath, "utf-8")))

        this.app = new Koa()
        this.router = new Router()
        this.fish = new Fish()

        await this.fish.setup()

        this.initRoutes()

        this.app.use(koaBodyParser())
        this.app.use(this.router.routes())
        this.app.listen(this.config.port)
    }
}


const fishServer = new FishServer();

fishServer.setup().then(r => console.log("Server Start"))
