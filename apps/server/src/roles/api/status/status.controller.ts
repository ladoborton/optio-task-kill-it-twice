import { Controller, Get } from '@nestjs/common';
import { Status, StatusService } from './status.service';

@Controller('api/status')
export class StatusController {
  constructor(private readonly status: StatusService) {}

  @Get()
  get(): Promise<Status> {
    return this.status.status();
  }
}
