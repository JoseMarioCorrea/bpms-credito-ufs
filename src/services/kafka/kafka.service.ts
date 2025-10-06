// src/services/kafka/primary-kafka.service.ts
import { Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import { Kafka, KafkaConfig, Producer, Consumer } from 'kafkajs';
import { ConfigService } from '@nestjs/config';

@Injectable()
export class PrimaryKafkaService implements OnModuleDestroy {
  private kafka: Kafka;
  private producer?: Producer;
  private consumers: Consumer[] = [];
  private readonly logger = new Logger(PrimaryKafkaService.name);

  constructor(private readonly config: ConfigService) {
    const brokersRaw = this.config.get<string>('KAFKA_BROKERS') || '';
    const clientId = this.config.get<string>('KAFKA_CLIENT_ID') || `proposal-facade-${process.pid}`;

    const brokers = brokersRaw
      .split(',')
      .map(b => b.trim())
      .filter(Boolean);

    if (brokers.length === 0) {
      throw new Error('KAFKA_BROKERS not set or empty (expected comma separated list)');
    }

    const kafkaConfig: KafkaConfig = { clientId, brokers };
    this.kafka = new Kafka(kafkaConfig);
    this.logger.log(`PrimaryKafkaService constructed (clientId=${clientId}, brokers=${brokers}, kafkaConfig=${kafkaConfig})`);
  }

  /** Conecta o producer (chame em bootstrap/init) */
  async init(): Promise<void> {
    if (this.producer) return;
    this.producer = this.kafka.producer();
    try {
      await this.producer.connect();
      this.logger.log('Primary Kafka producer connected');
    } catch (err) {
      this.logger.error('Primary Kafka producer connect error', err as any);
      throw err;
    }
  }

  async createConsumer(groupId: string, topic?: string, fromBeginning = false): Promise<Consumer> {
    if (!groupId) throw new Error('groupId is required');
    const consumer = this.kafka.consumer({ groupId, allowAutoTopicCreation: false });
    await consumer.connect();
    this.logger.log(`Primary Kafka consumer connected (group=${groupId})`);
    if (topic) {
      await consumer.subscribe({ topic, fromBeginning });
      this.logger.log(`Primary Kafka subscribed to topic=${topic}`);
    }
    this.consumers.push(consumer);
    return consumer;
  }

  // src/services/kafka/primary-kafka.service.ts
async runConsumer(
  groupId: string,
  topic: string,
  handler: ({ topic, partition, message }: { topic: string; partition: number; message: any }) => Promise<void>,
  fromBeginning = false,
  runOpts?: Parameters<Consumer['run']>[0],   // <— NOVO
): Promise<Consumer> {
  const consumer = await this.createConsumer(groupId, topic, fromBeginning);
  await consumer.run({
    autoCommit: runOpts?.autoCommit ?? false, // <— default: false
    eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
      try {
        await handler({ topic, partition, message });
      } catch (err) {
        this.logger.error('Handler error', err as any);
      }
    },
    ...runOpts,
  });
  return consumer;
}

  async send(topic: string, key: string | null, value: any) {
    if (!this.producer) throw new Error('Producer not initialized (call init())');
    const payload = typeof value === 'string' ? value : JSON.stringify(value);
    await this.producer.send({ topic, messages: [{ key: key ?? null, value: payload }] });
  }

  getProducer(): Producer | undefined {
    return this.producer;
  }

  async onModuleDestroy() {
    this.logger.log('PrimaryKafkaService shutting down');
    for (const c of this.consumers) {
      try { await c.disconnect(); } catch (e) { this.logger.warn('Error disconnecting consumer', e as any); }
    }
    if (this.producer) {
      try { await this.producer.disconnect(); } catch (e) { this.logger.warn('Error disconnecting producer', e as any); }
    }
  }
}
