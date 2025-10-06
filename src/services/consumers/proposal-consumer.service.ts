// src/consumers/proposal-consumer.service.ts
import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { Consumer } from 'kafkajs';
import { KafkaFacadeService } from '../../services/kafka/kafka-facade.service';
import { ConfigService } from '@nestjs/config';
import { ProposalOrchestratorService } from '../proposta-us/proposal-orchestrator.service';
import { OauthService } from '../../middlewares/unicred-auth/oauth.service';
import { randomUUID } from 'crypto';

@Injectable()
export class ProposalConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(ProposalConsumerService.name);
  private consumer: Consumer | null = null;
  private stopped = false;

  // retry config for SN publishes
  private readonly snRetryMax: number;
  private readonly snRetryBaseMs: number;

  constructor(
    private readonly kafkaFacade: KafkaFacadeService,
    private readonly config: ConfigService,
    private readonly orchestrator: ProposalOrchestratorService,
    private readonly oauth: OauthService,
  ) {
    this.snRetryMax = Number(this.config.get<number>('RETRY_MAX_ATTEMPTS') || 3);
    this.snRetryBaseMs = Number(this.config.get<number>('RETRY_BASE_MS') || 500);
  }

  // src/consumers/proposal-consumer.service.ts
  async onModuleInit(): Promise<void> {
    try {
      await this.kafkaFacade.initAll();
      this.logger.log('KafkaFacade initialized (primary connected; attempted SN)');
    } catch (err) {
      this.logger.warn('KafkaFacade.initAll() issue: ' + ((err as any)?.message || err));
    }

    const sourceTopic = this.config.get<string>('SOURCE_TOPIC') || 'credito-notificacao-situacao-proposta-v1';
    const groupId = this.config.get<string>('KAFKA_GROUP_ID') || 'proposal-facade-group-01';

    try {
      this.logger.log(`Starting consumer on topic=${sourceTopic} group=${groupId}`);
      this.consumer = await this.kafkaFacade.createPrimaryConsumer(groupId, sourceTopic, false);

      // ⚠️ autoCommit desativado
      await this.consumer.run({
        autoCommit: false,
        eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
          if (this.stopped) return;

          const offsetStr = message.offset;
          const nextOffset = (BigInt(offsetStr) + 1n).toString();

          try {
            await this.handleMessage({ topic, partition, message });

            // ✅ só confirma depois que a mensagem foi publicada na SN sem erro
            await this.consumer!.commitOffsets([{ topic, partition, offset: nextOffset }]);
            // Mantém o consumer vivo em lotes grandes
            await heartbeat?.();
          } catch (err) {
            const msg = (err as any)?.message || String(err);
            this.logger.error(`Erro no processamento (offset=${offsetStr}): ${msg}`);

            // Opcional: pausa curta pra evitar loop quente de erros
            try {
              pause?.();
              await new Promise(r => setTimeout(r, 500));
            } finally {
              // Sem commit → mensagem volta conforme rebalanço/retry
            }
          }
        },
      });
    } catch (err) {
      this.logger.error('Failed to start consumer: ' + ((err as any)?.message || err), err as any);
      throw err;
    }
  }


  private safeParse(value: string | null): any | null {
    if (!value) return null;
    try {
      return JSON.parse(value);
    } catch {
      return null;
    }
  }

  private sleep(ms: number) {
    return new Promise(res => setTimeout(res, ms));
  }

  private buildSnMessage(parsed: any, fullProposal: any, operationId?: string) {
    const numeroProposta = Number(parsed?.numeroProposta || parsed?.nrProposta || parsed?.proposta || parsed?.id);
    const coop = parsed?.cooperativa || this.config.get<string>('DEFAULT_COOPERATIVE') || null;
    return {
      schemaVersion: 'v1',
      origin: 'proposal-facade',
      operationId: operationId || parsed?.operationId || randomUUID(),
      numeroProposta,
      cooperativa: coop,
      situacao: parsed?.situacao || fullProposal?.proposta?.situacao || null,
      payloadMinimo: {
        cpfCnpj: fullProposal?.proposta?.cpfCnpj || null,
        conta: fullProposal?.proposta?.conta || null,
        linhaCredito: fullProposal?.proposta?.idLinhaCredito || null,
      },
      payloadCompleto: fullProposal,
      eventoEm: new Date().toISOString(),
      correlationId: parsed?.correlationId || null,
    };
  }

  /** send to SN with simple retry/backoff */
  private async sendToServiceNowWithRetry(topic: string, key: string | null, value: any) {
    let attempt = 0;
    const max = this.snRetryMax;
    const base = this.snRetryBaseMs;

    while (attempt <= max) {
      try {
        await this.kafkaFacade.sendToSN(topic, key, value);
        return; // ok
      } catch (err) {
        attempt++;
        const msg = (err as any)?.message || err;
        this.logger.warn(`Attempt #${attempt} to send to SN failed: ${msg}`);
        if (attempt > max) throw err;
        const backoff = base * attempt;
        await this.sleep(backoff);
      }
    }
  }

  private async handleMessage({ topic, partition, message }: { topic: string; partition: number; message: any }) {
    const rawValue = message.value ? message.value.toString() : null;
    const key = message.key ? message.key.toString() : null;

    this.logger.log(`Received message topic=${topic} partition=${partition} key=${key}`);
    const parsed = this.safeParse(rawValue);
    if (!parsed) { this.logger.warn('Invalid JSON message — skipping. Raw:', rawValue); return; }

    const numeroProposta = parsed?.numeroProposta || parsed?.nrProposta || parsed?.proposta || parsed?.id;
    if (!numeroProposta) { this.logger.warn('Message missing numeroProposta — skipping. Raw:', rawValue); return; }

    // 1) token Unicred
    let token: string;
    try { token = await this.oauth.getTokenUnicred(); }
    catch (err) { this.logger.error('Failed to obtain Unicred token: ' + ((err as any)?.message || err)); return; }

    const outgoingHeaders: Record<string, string> = {
      Authorization: `Bearer ${token}`,
      cooperativa: String(parsed?.cooperativa || this.config.get('DEFAULT_COOPERATIVE') || ''),
    };

    // 2) montar proposta
    let fullProposal: any;
    try {
      fullProposal = await this.orchestrator.buildFullProposal(Number(numeroProposta), outgoingHeaders);
    } catch (err) {
      this.logger.error('Error building full proposal: ' + ((err as any)?.message || err), err as any);
      const dlq = this.config.get<string>('DLQ_TOPIC');
      if (dlq) {
        try {
          await this.kafkaFacade.sendToPrimary(dlq, String(numeroProposta), { original: parsed, error: String((err as any)?.message || err) });
          this.logger.log(`Published build error to DLQ ${dlq}`);
        } catch (e) {
          this.logger.error('Failed to publish build error to DLQ: ' + ((e as any)?.message || e));
        }
      }
      // Sem throw — só não commitará no caller se você jogar erro; aqui vamos deixar retornar e o caller continua sem commit.
      throw err; // <— importante pra não commitar
    }

    // 3) log bonito
    this.logger.log(`Proposta montada: numeroProposta=${numeroProposta}`);

    // 4) publish em SN
    const snTopic =
      this.config.get<string>('TARGET_TOPIC') ||
      this.config.get<string>('KAFKA_TOPIC_SN_INTERNAL') ||
      'sn.unicred.proposal.status.v1';

    const messageToSend = this.buildSnMessage(parsed, fullProposal);

    try {
      await this.sendToServiceNowWithRetry(snTopic, String(numeroProposta), messageToSend);
      this.logger.log(`Forwarded proposal to SN topic=${snTopic} numeroProposta=${numeroProposta}`);
      // sucesso → commit acontece no caller
    } catch (err) {
      this.logger.error('Final failure sending to SN: ' + ((err as any)?.message || err));
      const dlq = this.config.get<string>('DLQ_TOPIC');
      if (dlq) {
        try {
          await this.kafkaFacade.sendToPrimary(dlq, String(numeroProposta), { original: messageToSend, error: String((err as any)?.message || err) });
          this.logger.log(`Published to DLQ ${dlq} numeroProposta=${numeroProposta}`);
        } catch (e) {
          this.logger.error('Failed to publish to DLQ after SN error: ' + ((e as any)?.message || e));
        }
      }
      throw err; // <— importante pra não commitar
    }
  }


  async onModuleDestroy(): Promise<void> {
    this.logger.log('ProposalConsumerService shutting down');
    this.stopped = true;
    try {
      if (this.consumer) {
        await this.consumer.disconnect();
        this.logger.log('Consumer disconnected');
      }
    } catch (err) {
      this.logger.warn('Error disconnecting consumer: ' + ((err as any)?.message || err));
    }
  }
}
