import { getQueueToken } from '@nestjs/bullmq';
import { Injectable } from '@nestjs/common';
import { ModuleRef, Reflector } from '@nestjs/core';
import { JobsOptions, Queue, Worker } from 'bullmq';
import { ClassConstructor } from 'class-transformer';
import { setTimeout } from 'node:timers/promises';
import { JobConfig } from 'src/decorators';
import { JobName, JobStatus, MetadataKey, QueueCleanType, QueueName } from 'src/enum';
import { ConfigRepository } from 'src/repositories/config.repository';
import { EventRepository } from 'src/repositories/event.repository';
import { LoggingRepository } from 'src/repositories/logging.repository';
import { IEntityJob, JobCounts, JobItem, JobOf, QueueStatus } from 'src/types';
import { getKeyByValue, getMethodNames, ImmichStartupError } from 'src/utils/misc';
import { Kysely } from 'kysely';
import { InjectKysely } from 'nestjs-kysely';
import { DB } from 'src/db';
import { GenerateSql } from 'src/decorators';
import { driver } from "src/utils/database";

type JobMapItem = {
  jobName: JobName;
  queueName: QueueName;
  handler: (job: JobOf<any>) => Promise<JobStatus>;
  label: string;
};

@Injectable()
export class JobRepository {
  private handlers: Partial<Record<JobName, JobMapItem>> = {};

  constructor(
    @InjectKysely() private db: Kysely<DB>,
    private logger: LoggingRepository,
    private moduleRef: ModuleRef,
    private eventRepository: EventRepository,
  ) {
    this.logger.setContext(JobRepository.name);
  }

  setup(services: ClassConstructor<unknown>[]) {
    const reflector = this.moduleRef.get(Reflector, { strict: false });

    for (const service of services) {
      const instance = this.moduleRef.get<any>(service);
      for (const methodName of getMethodNames(instance)) {
        const handler = instance[methodName];
        const config = reflector.get<JobConfig>(MetadataKey.JOB_CONFIG, handler);
        if (!config) {
          continue;
        }

        const { name: jobName, queue: queueName } = config;
        const label = `${service.name}.${handler.name}`;

        // one handler per job
        if (this.handlers[jobName]) {
          const jobKey = getKeyByValue(JobName, jobName);
          const errorMessage = `Failed to add job handler for ${label}`;
          this.logger.error(
            `${errorMessage}. JobName.${jobKey} is already handled by ${this.handlers[jobName].label}.`,
          );
          throw new ImmichStartupError(errorMessage);
        }

        this.handlers[jobName] = {
          label,
          jobName,
          queueName,
          handler: handler.bind(instance),
        };

        this.logger.verbose(`Added job handler: ${jobName} => ${label}`);
      }
    }
  }

  async startWorkers() {
    for (const queueName of Object.values(QueueName)) {
      this.logger.log(`Starting worker for queue: ${queueName}`);
      // mostly not needed with triggers, only really used as a fallback
      this.loop(queueName);
    }

    this.logger.log(`driver is ${driver}`);

    if (driver) {
      this.logger.log(`Starting listener`);
      driver.listen('jobs', async (queueName: string) => {
        this.logger.log(`Received notification for ${queueName}`);

        // TODO: Might be better to wake workers up via Promise.all instead
        await this.do(queueName as QueueName);
      });
    }
  }

  // use skip for locked
  async loop(queue: QueueName): Promise<void> {
    while (true) {
      await this.do(queue);
    }
  }

  async do(queue: QueueName): Promise<void> {
    const result = await this.db.transaction().execute(async (db: Kysely<DB>) => {
      const items: { id: string }[] = await db
        .selectFrom('jobs')
        .select(['id'])
        .where('queue', '=', queue)
        .forUpdate()
        .skipLocked()
        .limit(1)
        .execute();

      if (!items.length) {
        // this.logger.log(`No jobs found in queue: ${queue}`);
        return null;
      }

      return await db.
        deleteFrom('jobs')
        .where('id', '=', items[0].id)
        .returningAll()
        .executeTakeFirst();
    });

    if (!result) {
      // this.logger.log(`No jobs found in queue (result): ${queue}`);
      // TODO: should be configurable.
      await setTimeout(5000);
      return;
    }

    this.logger.log(`Processing job: ${result.name}, data: ${result.data}`);

    result.data = JSON.parse(result.data);

    await this.eventRepository.emit('job.start', queue, result as JobItem)
  }

  async run({ name, data }: { name: string, data?: any }): Promise<JobStatus> {
    const item = this.handlers[name as JobName];
    if (!item) {
      this.logger.warn(`Skipping unknown job: "${name}"`);
      return JobStatus.SKIPPED;
    }

    return item.handler(data);
  }

  @GenerateSql({ params: [] })
  async queue(item: JobItem): Promise<void> {
    this.logger.log(`Queueing job: ${item.name}, data: ${JSON.stringify(item.data)}`);
    this.db.insertInto('jobs').values(this.itemValue(item)).execute();
  }

  @GenerateSql({ params: [] })
  async queueAll(items: JobItem[]): Promise<void> {
    if (items.length === 0) {
      this.logger.log(`empty array? ${JSON.stringify(items)}`);
      return;
    }
    this.logger.log(`Queueing jobs: ${items.map((item) => item.name).join(', ')}`);
    this.db.insertInto('jobs').values(items.map(this.itemValue.bind(this))).execute();
  }

  private itemValue(item: JobItem): any {
    return {
      queue: this.queueName(item.name),
      name: item.name,
      status: JobStatus.PENDING,
      data: JSON.stringify(item.data),
    }
  }

  private queueName(name: JobName): string {
    return (this.handlers[name] as JobMapItem).queueName;
  }

  async pause(name: QueueName): Promise<void> { }

  async resume(name: QueueName): Promise<void> { }

  async empty(name: QueueName): Promise<void> {
    await this.db.deleteFrom('jobs')
      .where('queue', '=', name)
      .where('status', '=', JobStatus.ACTIVE)
      .execute();
  }

  async clear(name: QueueName, type: QueueCleanType): Promise<void> {
    await this.db.deleteFrom('jobs')
      .where('queue', '=', name)
      .where('status', '=', JobStatus.PENDING)
      .execute();
  }

  async waitForQueueCompletion(...queues: QueueName[]): Promise<void> { }

  async getJobCounts(name: QueueName): Promise<JobCounts> {
    // count with sql
    const result: {
      status: string,
      count: string | number | bigint,
    }[] = await this.db
      .selectFrom('jobs')
      .select(['status', this.db.fn.count('id').as('count')])
      .where('queue', '=', name)
      .groupBy('status')
      .execute();

    const counts = result.reduce((acc: Record<JobStatus, Number>, row) => {
      acc[row.status as JobStatus] = Number(row.count);
      return acc;
    }, {} as Record<JobStatus, number>);

    return <JobCounts>{
      active: counts[JobStatus.ACTIVE] || 0,
      failed: 0,
      waiting: counts[JobStatus.PENDING] || 0,
      paused: 0,
    }
  }

  async removeJob(jobId: string, name: JobName): Promise<IEntityJob | undefined> {
    await this.db.deleteFrom('jobs')
      .where('id', '=', jobId)
      .execute();
    return undefined;
  }

  async getQueueStatus(name: QueueName): Promise<QueueStatus> {
    const result = await this.db
      .selectFrom('jobs')
      .select(['status'])
      .where('queue', '=', name)
      .where('status', '=', JobStatus.ACTIVE)
      .limit(1)
      .execute();
    return {
      isActive: result.length > 0,
      isPaused: false,
    }
  }

  async setConcurrency(queueName: QueueName, concurrency: number): Promise<void> { }
}

@Injectable()
export class JobRepositoryOld {
  private workers: Partial<Record<QueueName, Worker>> = {};
  private handlers: Partial<Record<JobName, JobMapItem>> = {};

  constructor(
    private moduleRef: ModuleRef,
    private configRepository: ConfigRepository,
    private eventRepository: EventRepository,
    private logger: LoggingRepository,
  ) {
    this.logger.setContext(JobRepository.name);
  }

  setup({ services }: { services: ClassConstructor<unknown>[] }) {
    const reflector = this.moduleRef.get(Reflector, { strict: false });

    for (const service of services) {
      const instance = this.moduleRef.get<any>(service);
      for (const methodName of getMethodNames(instance)) {
        const handler = instance[methodName];
        const config = reflector.get<JobConfig>(MetadataKey.JOB_CONFIG, handler);
        if (!config) {
          continue;
        }

        const { name: jobName, queue: queueName } = config;
        const label = `${service.name}.${handler.name}`;

        // one handler per job
        if (this.handlers[jobName]) {
          const jobKey = getKeyByValue(JobName, jobName);
          const errorMessage = `Failed to add job handler for ${label}`;
          this.logger.error(
            `${errorMessage}. JobName.${jobKey} is already handled by ${this.handlers[jobName].label}.`,
          );
          throw new ImmichStartupError(errorMessage);
        }

        this.handlers[jobName] = {
          label,
          jobName,
          queueName,
          handler: handler.bind(instance),
        };

        this.logger.verbose(`Added job handler: ${jobName} => ${label}`);
      }
    }

    // no missing handlers
    for (const [jobKey, jobName] of Object.entries(JobName)) {
      const item = this.handlers[jobName];
      if (!item) {
        const errorMessage = `Failed to find job handler for Job.${jobKey} ("${jobName}")`;
        this.logger.error(
          `${errorMessage}. Make sure to add the @OnJob({ name: JobName.${jobKey}, queue: QueueName.XYZ }) decorator for the new job.`,
        );
        throw new ImmichStartupError(errorMessage);
      }
    }
  }

  startWorkers() {
    const { bull } = this.configRepository.getEnv();
    for (const queueName of Object.values(QueueName)) {
      this.logger.debug(`Starting worker for queue: ${queueName}`);
      this.workers[queueName] = new Worker(
        queueName,
        (job) => this.eventRepository.emit('job.start', queueName, job as JobItem),
        { ...bull.config, concurrency: 1 },
      );
    }
  }

  async run({ name, data }: JobItem) {
    const item = this.handlers[name as JobName];
    if (!item) {
      this.logger.warn(`Skipping unknown job: "${name}"`);
      return JobStatus.SKIPPED;
    }

    return item.handler(data);
  }

  setConcurrency(queueName: QueueName, concurrency: number) {
    const worker = this.workers[queueName];
  }

  async getQueueStatus(name: QueueName): Promise<QueueStatus> {
    const queue = this.getQueue(name);

    return {
      isActive: !!(await queue.getActiveCount()),
      isPaused: await queue.isPaused(),
    };
  }

  pause(name: QueueName) {
    return this.getQueue(name).pause();
  }

  resume(name: QueueName) {
    return this.getQueue(name).resume();
  }

  empty(name: QueueName) {
    return this.getQueue(name).drain();
  }

  clear(name: QueueName, type: QueueCleanType) {
    return this.getQueue(name).clean(0, 1000, type);
  }

  getJobCounts(name: QueueName): Promise<JobCounts> {
    return this.getQueue(name).getJobCounts(
      'active',
      'failed',
      'waiting',
      'paused',
    ) as unknown as Promise<JobCounts>;
  }

  private getQueueName(name: JobName) {
    return (this.handlers[name] as JobMapItem).queueName;
  }

  async queueAll(items: JobItem[]): Promise<void> {
    if (items.length === 0) {
      return;
    }

    const promises = [];
    const itemsByQueue = {} as Record<string, (JobItem & { data: any; options: JobsOptions | undefined })[]>;
    for (const item of items) {
      const queueName = this.getQueueName(item.name);
      const job = {
        name: item.name,
        data: item.data || {},
        options: this.getJobOptions(item) || undefined,
      } as JobItem & { data: any; options: JobsOptions | undefined };

      if (job.options?.jobId) {
        // need to use add() instead of addBulk() for jobId deduplication
        promises.push(this.getQueue(queueName).add(item.name, item.data, job.options));
      } else {
        itemsByQueue[queueName] = itemsByQueue[queueName] || [];
        itemsByQueue[queueName].push(job);
      }
    }

    for (const [queueName, jobs] of Object.entries(itemsByQueue)) {
      const queue = this.getQueue(queueName as QueueName);
      promises.push(queue.addBulk(jobs));
    }

    await Promise.all(promises);
  }

  async queue(item: JobItem): Promise<void> {
    return this.queueAll([item]);
  }

  async waitForQueueCompletion(...queues: QueueName[]): Promise<void> {
    let activeQueue: QueueStatus | undefined;
    do {
      const statuses = await Promise.all(queues.map((name) => this.getQueueStatus(name)));
      activeQueue = statuses.find((status) => status.isActive);
    } while (activeQueue);
    {
      this.logger.verbose(`Waiting for ${activeQueue} queue to stop...`);
      await setTimeout(1000);
    }
  }

  private getJobOptions(item: JobItem): JobsOptions | null {
    switch (item.name) {
      case JobName.NOTIFY_ALBUM_UPDATE: {
        return { jobId: item.data.id, delay: item.data?.delay };
      }
      case JobName.STORAGE_TEMPLATE_MIGRATION_SINGLE: {
        return { jobId: item.data.id };
      }
      case JobName.GENERATE_PERSON_THUMBNAIL: {
        return { priority: 1 };
      }
      case JobName.QUEUE_FACIAL_RECOGNITION: {
        return { jobId: JobName.QUEUE_FACIAL_RECOGNITION };
      }
      default: {
        return null;
      }
    }
  }

  private getQueue(queue: QueueName): Queue {
    return this.moduleRef.get<Queue>(getQueueToken(queue), { strict: false });
  }

  public async removeJob(jobId: string, name: JobName): Promise<IEntityJob | undefined> {
    const existingJob = await this.getQueue(this.getQueueName(name)).getJob(jobId);
    if (!existingJob) {
      return;
    }
    try {
      await existingJob.remove();
    } catch (error: any) {
      if (error.message?.includes('Missing key for job')) {
        return;
      }
      throw error;
    }
    return existingJob.data;
  }
}
