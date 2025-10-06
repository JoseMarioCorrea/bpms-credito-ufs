// src/services/kafka/kafka-facade.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { PrimaryKafkaService } from './kafka.service';
import { ServiceNowKafkaService } from './snow-kafka.service';

@Injectable()
export class KafkaFacadeService {
  private readonly logger = new Logger(KafkaFacadeService.name);

  constructor(
    private readonly primary: PrimaryKafkaService,
    private readonly snow: ServiceNowKafkaService,
  ) {}

  /** Inicializa Primary (obrigatório) e tenta inicializar SN (opcional) */
  async initAll(): Promise<void> {
    await this.primary.init();
    try {
      await this.snow.init();
    } catch (e) {
      this.logger.warn('ServiceNow Kafka init failed (continuing with primary): ' + (e?.message || e));
    }
  }

  async sendToPrimary(topic: string, key: string | null, value: any) {
    return this.primary.send(topic, key, value);
  }

  async sendToSN(topic: string, key: string | null, value: any) {
    return this.snow.sendToServiceNow(topic, key, value);
  }

  createPrimaryConsumer(...args: Parameters<PrimaryKafkaService['createConsumer']>) {
    return this.primary.createConsumer(...args);
  }

  runPrimaryConsumer(...args: Parameters<PrimaryKafkaService['runConsumer']>) {
    return this.primary.runConsumer(...args);
  }
}
