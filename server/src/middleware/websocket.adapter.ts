import { INestApplicationContext } from '@nestjs/common';
import { IoAdapter } from '@nestjs/platform-socket.io';
import { createAdapter } from '@socket.io/postgres-adapter';
import { ServerOptions } from 'socket.io';
import { ConfigRepository } from 'src/repositories/config.repository';
import pg, { PoolConfig } from 'pg';

export class WebSocketAdapter extends IoAdapter {
  constructor(private app: INestApplicationContext) {
    super(app);
  }

  createIOServer(port: number, options?: ServerOptions): any {
    const server = super.createIOServer(port, options);
    const configRepository = new ConfigRepository();
    const { database } = configRepository.getEnv();
    const pool = new pg.Pool(database.config.kysely as PoolConfig);
    server.adapter(createAdapter(pool));
    return server;
  }
}
