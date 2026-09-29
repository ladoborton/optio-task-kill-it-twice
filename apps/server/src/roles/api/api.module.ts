import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { dataSourceOptions } from '../../database/data-source';
import { esClientProvider } from '../../shared/elasticsearch';
import { ControlController } from './control/control.controller';
import { DataController } from './data/data.controller';
import { DlqController } from './dlq/dlq.controller';
import { DockerClient } from './sim/docker.client';
import { SimController } from './sim/sim.controller';
import { StatusController } from './status/status.controller';
import { StatusService } from './status/status.service';

@Module({
  imports: [TypeOrmModule.forRoot(dataSourceOptions)],
  controllers: [StatusController, ControlController, DlqController, DataController, SimController],
  providers: [StatusService, DockerClient, esClientProvider],
})
export class ApiModule {}
