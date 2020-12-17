import * as jieba from "nodejieba"
import * as fs from "fs"
import * as child_process from "child_process"
import * as sonic from "sonic-channel"
import * as path from "path"
import * as cldr from "cldr-segmentation"
import * as chardet from "chardet"
import * as iconv from "iconv-lite"
import * as officeParser from "officeparser"
import parsePDF from "pdf-extraction"
import glob from "glob"
import parseDuration from "parse-duration"
import parseByte from "byte-parser"
import AsyncLock from "async-lock"
import docx4js from "docx4js"

import Timeout = NodeJS.Timeout;
import fp = fs.promises;

type FilePath = string

interface FileTypeConfig {
    name: string
    ext: Array<string>
    sizeLimit: string
}

interface SonicConfig {
    host: string
    port: number
    password: string
    exePath: string
    cfgPath: string
}

export interface FishConfig {
    sonic: SonicConfig
    types: Array<FileTypeConfig>
    initialOffset: number
    wordLimit: number
    exclude: Array<string>
}

export interface FishTask {
    name: string
    alias: string
    directory: FilePath
    include: string
    exclude: string
    schedule: string
}

type FishTaskConfig = Array<FishTask>

interface SonicChannelGroup {
    searchChannel: sonic.Search
    ingestChannel: sonic.Ingest
    controlChannel: sonic.Control
}

interface FileLog {
    filePath: FilePath
    lastModified: number
}

interface TaskLog {
    lastRun: number
    lastFiles: Array<FileLog>
}

interface FishTaskWithLog {
    name: string
    alias: string
    directory: FilePath
    include: string
    exclude: string
    schedule: string
    lastRun: number
    lastFiles: Array<FileLog>
}

interface ScannerInfo {
    filePath: string
    fileStats: fs.Stats
}

type FileScanner = (self: Fish, info: ScannerInfo) => Promise<string>

const lock = AsyncLock()

async function docxScanner(self : Fish, info : ScannerInfo) : Promise<string> {
    const config = self.typeConfigByName("docx")

    if (config === undefined)
        throw new Error("undefined config")

    if (info.fileStats.size > parseByte(config.sizeLimit))
        throw new Error("file oversize.")

    const docx = await docx4js.load(info.filePath)

    const content = docx.officeDocument.content("w\\:t")
    const length = content.length

    let text : any[] = []

    for(let i = 0; i < length; i++)
        text = text.concat(content[i].children)

    return text
        .map(v => v.data.trim())
        .filter(v => {
            return v.length >= self.getConfig().wordLimit
        })
        .reduce((acc, v) => acc + " " + v, "")
}

async function officeScanner(self: Fish, info: ScannerInfo): Promise<string> {
    const config = self.typeConfigByName("office")

    if (config === undefined)
        throw new Error("undefined config")

    if (info.fileStats.size > parseByte(config.sizeLimit))
        throw new Error("file oversize.")

    return new Promise((resolve, reject) => {
        lock.acquire("office", callback => {
            console.log(info)

            officeParser.parseOffice(info.filePath, callback)
        }, (data, err) => {
            console.log(info)

            if (err) reject(err)
            else resolve(data)
        })
    })
}

async function textScanner(self: Fish, info: ScannerInfo): Promise<string> {
    const config = self.typeConfigByName("text")

    if (config === undefined)
        throw new Error("undefined config")

    if (info.fileStats.size > parseByte(config.sizeLimit))
        throw new Error("file oversize.")

    const fileContent = await fp.readFile(info.filePath)

    const encoding = chardet.detect(fileContent)

    if (encoding === null)
        throw new Error("undefined encoding")

    return iconv.decode(fileContent, encoding)
}

async function pdfScanner(self: Fish, info: ScannerInfo): Promise<string> {
    const config = self.typeConfigByName("pdf")

    if (config === undefined)
        throw new Error("undefined config")

    if (info.fileStats.size > parseByte(config.sizeLimit))
        throw new Error("file oversize.")

    const fileContent = await fp.readFile(info.filePath)

    const pdf = await parsePDF(fileContent)

    return pdf.text
}


export class Fish {
    private readonly configPath: FilePath = "./config.json"
    private readonly taskConfigPath: FilePath = "./task.json"
    private readonly logDirPath: FilePath = "./log"

    // @ts-ignore
    private config: FishConfig
    // @ts-ignore
    private taskConfig: FishTaskConfig
    // @ts-ignore
    private sonicProcess: child_process.ChildProcess
    // @ts-ignore
    private sonicChannels: SonicChannelGroup
    // @ts-ignore
    private delayedQueue: Array<Timeout>
    // @ts-ignore
    private fileScanner: Map<string, FileScanner>

    async connectChannel(channel: sonic.Search | sonic.Ingest | sonic.Control) {
        return new Promise((resolve, reject) => {
            channel.connect({
                connected: () => resolve(channel),
                timeout: () => reject(channel),
                error: () => reject(channel)
            })
        })
    }

    async runChannels(): Promise<SonicChannelGroup> {
        const options: sonic.Options = {
            host: this.config.sonic.host,
            port: this.config.sonic.port,
            auth: this.config.sonic.password
        }

        const channels = await Promise.all([
            new sonic.Search(options),
            new sonic.Ingest(options),
            new sonic.Control(options)
        ].map(async channel => this.connectChannel(channel)))

        console.log("Channels has connected.")

        return {
            searchChannel: channels[0] as sonic.Search,
            ingestChannel: channels[1] as sonic.Ingest,
            controlChannel: channels[2] as sonic.Control
        }
    }

    async runSonic(): Promise<child_process.ChildProcess> {
        return new Promise((resolve, reject) => {
            const process = child_process.execFile(
                this.config.sonic.exePath,
                ["-c", this.config.sonic.cfgPath],
                (err) => {
                    if (err) reject(err)
                })

            // 预计启动时间
            setTimeout(() => {
                resolve(process)
            }, 200)
        })
    }

    async readTaskLog(task: string): Promise<TaskLog> {
        const logPath = path.join(this.logDirPath, `${task}.json`)

        if (!fs.existsSync(logPath))
            return {lastRun: 0, lastFiles: []}

        return JSON.parse(await fp.readFile(logPath, "utf-8")) as TaskLog
    }

    async writeTaskLog(task: string, log: TaskLog): Promise<void> {
        const logPath = path.join(this.logDirPath, `${task}.json`)

        await fp.writeFile(logPath, JSON.stringify(log, null, 4))
    }

    async walkDir(directory: string, include: string, exclude: string): Promise<Array<string>> {
        return new Promise((resolve, reject) => {
            const ignore = exclude.trim().length === 0 ? this.config.exclude : exclude

            glob(
                include,
                {
                    cwd: directory,
                    ignore: ignore,
                    absolute: true,
                    matchBase: true
                },
                (err, files) => {
                    if (err) reject(err);
                    else resolve(files);
                }
            );
        });
    }

    base64Encode(message: string): string {
        return encodeURI(Buffer.from(message).toString("base64"))
    }

    base64Decode(message: string): string {
        return decodeURI(Buffer.from(message, "base64").toString())
    }

    async flushFile(task: FishTask, file: string): Promise<number> {
        return await this.sonicChannels.ingestChannel.flusho(task.name, "content", this.base64Encode(file))
    }

    typeConfigByName(name: string): FileTypeConfig | undefined {
        for (const type of this.config.types) {
            if (type.name === name)
                return type
        }

        return undefined
    }

    typeConfigByExt(ext: string): FileTypeConfig | undefined {
        for (const type of this.config.types) {
            if (type.ext.includes(ext))
                return type
        }

        return undefined
    }

    scanner(ext: string): FileScanner | undefined {
        const config = this.typeConfigByExt(ext);

        if (config === undefined)
            return undefined

        return this.fileScanner.get(config.name)
    }

    preprocessContent(content: string): string {
        const jiebaResult : string[] = jieba.cutAll(content)
        const cldrResult : string[] = cldr.wordSplit(content)

        const fullResult = jiebaResult.concat(cldrResult)

        const result = fullResult.map(v => v.trim()).filter(v => {
            return v.length >= this.config.wordLimit
        })

        return Array
            .from(new Set(result))
            .reduce((acc, v) => acc + " " + v, "")
    }

    async scanFile(task: FishTask, file: ScannerInfo): Promise<void> {
        const {filePath} = file;

        const extension = path.extname(filePath)
        const baseName = path.basename(filePath)

        const key = this.base64Encode(filePath)
        const scanner = this.scanner(extension);

        if (scanner == undefined)
            throw new Error("undefined scanner")

        try {
            const chunkSubstr = (str, size) => {
                const numChunks = Math.ceil(str.length / size)
                const chunks = new Array(numChunks)

                for (let i = 0, o = 0; i < numChunks; ++i, o += size) {
                    chunks[i] = str.substr(o, size)
                }

                return chunks
            }

            const chunks = chunkSubstr(await scanner(this, file), 2000)

            for(const chunk of chunks) {
                const content = this.preprocessContent(chunk)

                await this.sonicChannels.ingestChannel.push(task.alias, "content", key, content, {
                    lang: "none"
                })
            }
        } catch (e) {
            throw e
        }
    }

    async doTaskRemoveAction(task: FishTask, files: Array<string>): Promise<number> {
        let counter : number = 0

        for(const file of files) {
            try {
                await this.flushFile(task, file)

                counter++
            } catch (e) {
            }
        }

        return counter
    }

    async doTaskUpdateAction(task: FishTask, files: Array<ScannerInfo>) : Promise<number> {
        const length = files.length
        const name = task.name;

        let counter : number = 0

        for(const file of files) {

            try {
                console.log(`${name} START: ${file.filePath}`)

                await this.scanFile(task, file)

                counter++

                console.log(`${name} DONE: ${file.filePath}`)
            } catch (e) {
                console.log(`${name} ERROR: ${file.filePath}`)
                console.log(e)
            }

            console.log(`${name} S: ${counter} E: ${length - counter} T: ${length}`)
        }

        return counter
    }

    async doTask(task: FishTaskWithLog, force: boolean = false) {
        console.log(`Task Start: ${task.name}`)

        const diff = (a, b) => a.filter(x => !b.includes(x));

        const files = await this.walkDir(task.directory, task.include, task.exclude)

        const filesWithStats = await Promise.all(
            files.map(async file => {
                return {
                    filePath: file,
                    fileStats: await fp.stat(file)
                }
            })
        )

        const removedFiles = diff(
            task.lastFiles.map(v => v.filePath),
            files
        )

        await this.doTaskRemoveAction(task, removedFiles)

        const filePathMapping = new Map<string, number>()

        for (const file of task.lastFiles)
            filePathMapping.set(file.filePath, file.lastModified)


        const modifiedFiles = filesWithStats.filter(v => {
            const {filePath, fileStats: {mtime}} = v;

            if (force) return true;

            const recordModified = filePathMapping.get(filePath);

            if (recordModified === undefined) return true;

            return mtime.getTime() > recordModified;
        })

        await this.doTaskUpdateAction(task, modifiedFiles);

        await this.writeTaskLog(task.name, {
            lastRun: Date.now(),
            lastFiles: filesWithStats.map(v => {
                return {
                    filePath: v.filePath,
                    lastModified: v.fileStats.mtime.getTime()
                }
            })
        })

        console.log(`Task End: ${task.name}`)

        const duration = parseDuration(task.schedule)

        if (duration === null)
            throw new Error("invalid duration")

        this.submitDelayTask(task, duration)
    }

    submitDelayTask(task: FishTaskWithLog, delay: number): Timeout {
        console.log(`Schedule Task: ${task.name}`)

        const timer = setTimeout(() => this.doTask(task), delay)

        this.delayedQueue.push(timer)

        return timer
    }

    combineTaskWithLog(task: FishTask, log: TaskLog): FishTaskWithLog {
        return {
            ...task,
            ...log
        }
    }

    async initialTasks() {
        console.log(`Total Tasks: ${this.taskConfig.length}`)

        const preprocessedTasks: Array<FishTaskWithLog> = await Promise.all(
            this.taskConfig.map(async task => {
                return this.combineTaskWithLog(task, await this.readTaskLog(task.name))
            })
        )

        this.delayedQueue = []

        for (const task of preprocessedTasks) {
            const lastRun = task.lastRun
            const run = Date.now()
            const duration = parseDuration(task.schedule) || 0

            if ((run - lastRun) >= this.config.initialOffset + duration) {
                await this.doTask(task)
            } else {
                this.submitDelayTask(task, lastRun + duration - run)
            }
        }

        console.log(`Queued Tasks: ${this.delayedQueue.length}`)
    }

    setupLog() {
        // @ts-ignore
        this.sonicProcess.stdout.on("data", data => {
            console.log(`[Sonic]:\n${data}`);
        });

        // @ts-ignore
        this.sonicProcess.stderr.on("data", data => {
            console.error(`[Sonic]:\n${data}`);
        });

        this.sonicProcess.on("close", code => {
            console.log(`[Sonic]: process exited with code ${code}`);
        });
    }

    async setup(): Promise<this> {
        // 读取配置文件
        this.config = JSON.parse(await fp.readFile(this.configPath, "utf-8")) as FishConfig
        this.taskConfig = JSON.parse(await fp.readFile(this.taskConfigPath, "utf-8")) as FishTaskConfig

        if (!fs.existsSync(this.logDirPath))
            await fp.mkdir(this.logDirPath)

        if (!fs.existsSync("./data"))
            await fp.mkdir("./data")

        // Setup Scanner
        this.fileScanner = new Map<string, FileScanner>()
        this.fileScanner.set("docx", docxScanner)
        this.fileScanner.set("office", officeScanner)
        this.fileScanner.set("pdf", pdfScanner)
        this.fileScanner.set("text", textScanner)

        this.sonicProcess = await this.runSonic()
        this.sonicChannels = await this.runChannels()
        this.setupLog()

        await jieba.load()

        await this.initialTasks()

        return this
    }

    async query(alias: string, keyword: string): Promise<Array<string>> {
        return (await this.sonicChannels.searchChannel
            .query(alias, "content", keyword))
            .map(v => this.base64Decode(v))
    }

    async queryInAll(keyword: string): Promise<Array<string>> {
        return (await Promise
            .all(this.taskConfig
                .map(v => v.alias)
                .map(async v => await this.query(v, keyword))))
            .reduce((acc, v) => acc.concat(v), [])
    }

    async flushConfig() {
        await fp.writeFile(this.configPath, JSON.stringify(this.config, null, 4))
        await fp.writeFile(this.taskConfigPath, JSON.stringify(this.taskConfig, null, 4))
    }

    async shutdown() {
        await this.sonicChannels.searchChannel.close()
        await this.sonicChannels.ingestChannel.close()
        await this.sonicChannels.controlChannel.close()

        this.sonicProcess.kill()

        this.delayedQueue.forEach(v => clearTimeout(v))
    }

    /*
        Config Interface
     */

    getTasks(): FishTaskConfig {
        return this.taskConfig
    }

    async removeTask(name: string) {
        this.taskConfig = this
            .taskConfig
            .filter(v => v.name !== name)

        await this.flushConfig()
    }

    async addTask(task: FishTask) {
        this.taskConfig.push(task)

        await this.flushConfig()

        await this.sudoTask(task.name)
    }

    async getTaskInfo(name: string): Promise<FishTaskWithLog> {
        const task = this.taskConfig.find(v => v.name === name);

        if (task === undefined)
            throw new Error("undefined task")

        const log = await this.readTaskLog(task.name)

        return this.combineTaskWithLog(task, log)
    }

    getConfig(): FishConfig {
        return this.config
    }

    async updateConfig(config: FishConfig) {
        this.config = config

        await this.flushConfig()
    }

    async sudoTask(name: string) {
        const taskWithLog = await this.getTaskInfo(name)

        await this.doTask(taskWithLog, true)
    }
}
