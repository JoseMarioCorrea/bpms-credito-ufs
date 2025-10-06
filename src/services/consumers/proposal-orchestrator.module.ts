// src/consumers/consumers.module.ts
import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { ProposalConsumerService } from './proposal-consumer.service';
import { KafkaFacadeService } from '../../services/kafka/kafka-facade.service';
import { PrimaryKafkaService } from '../../services/kafka/kafka.service';
import { ServiceNowKafkaService } from '../../services/kafka/snow-kafka.service';
import { ProposalOrchestratorService } from '../proposta-us/proposal-orchestrator.service';
import { OauthService } from '../../middlewares/unicred-auth/oauth.service';
import { KafkaModule } from '../kafka/kafka.module';

@Module({
  imports: [ConfigModule, KafkaModule],
  providers: [
    ProposalConsumerService,
    KafkaFacadeService,
    PrimaryKafkaService,
    ServiceNowKafkaService,
    ProposalOrchestratorService,
    OauthService,
  ],
  exports: [ProposalConsumerService],
})
export class ProposalOrchestratorModule {}

export { ProposalConsumerService };
