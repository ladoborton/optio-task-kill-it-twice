import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { dataSourceOptions } from '../../database/data-source';
import { StatusController } from './status/status.controller';
import { StatusService } from './status/status.service';

@Module({
  imports: [TypeOrmModule.forRoot(dataSourceOptions)],
  controllers: [StatusController],
  providers: [StatusService],
})
export class ApiModule {}
