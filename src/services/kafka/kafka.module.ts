import { Module, OnModuleInit } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { PrimaryKafkaService } from './kafka.service';
import { ServiceNowKafkaService } from './snow-kafka.service';
import { KafkaFacadeService } from './kafka-facade.service';
import { ProposalOrchestratorService } from '../proposta-us/proposal-orchestrator.service';
import { HttpModule } from '@nestjs/axios';
import { ProposalConsumerService } from '../consumers/proposal-consumer.service';
import { OauthService } from 'src/middlewares/unicred-auth/oauth.service';

@Module({
   imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    HttpModule.registerAsync({
      useFactory: () => ({
        timeout: 15000,
        maxRedirects: 0,
      }),
    }),
  ],
    providers: [
    ProposalConsumerService,
    ProposalOrchestratorService,
    KafkaFacadeService,
    PrimaryKafkaService,
    ServiceNowKafkaService,
    OauthService,
  ],
  exports: [PrimaryKafkaService, ServiceNowKafkaService, KafkaFacadeService, ProposalOrchestratorService],
})
export class KafkaModule implements OnModuleInit {
  constructor(private readonly facade: KafkaFacadeService) {}
  async onModuleInit() {
    // Init producer(s) early so rest of app can use them
    await this.facade.initAll();
  }
}
