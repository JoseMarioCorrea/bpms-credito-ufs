// src/services/kafka/service-now-kafka.service.ts
import { Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import { Kafka, KafkaConfig, Admin, Producer, logLevel, Partitioners } from 'kafkajs';
import { ConfigService } from '@nestjs/config';
import * as fs from 'fs';
import * as path from 'path';

type TLSMin = 'TLSv1.2' | 'TLSv1.3';

@Injectable()
export class ServiceNowKafkaService implements OnModuleDestroy {
  private kafka?: Kafka;
  private admin?: Admin;
  private producer?: Producer;
  private connected = false;
  private initializing = false;
  private readonly logger = new Logger(ServiceNowKafkaService.name);

  constructor(private readonly config: ConfigService) {
    const rawBrokers =
      this.config.get<string>('KAFKA_SNOW_BROKERS') ||
      'unicreddev.service-now.com:4000,unicreddev.service-now.com:4001,unicreddev.service-now.com:4002,unicreddev.service-now.com:4003';

    const brokers = rawBrokers.split(',').map(s => s.trim()).filter(Boolean);
    if (!brokers.length) {
      this.logger.warn('KAFKA_SNOW_BROKERS vazio — ServiceNowKafka desabilitado');
      return;
    }

    const clientId = this.config.get<string>('KAFKA_SNOW_CLIENT_ID') || `sn-${process.pid}`;
    const servername = this.firstHostname(brokers) || 'unicreddev.service-now.com';

    const ssl = this.buildSSLOptions(servername);
    const sasl = this.buildSASL();

    const cfg: KafkaConfig = {
      clientId,
      brokers,
      logLevel: logLevel.INFO,
      connectionTimeout: this.num('KAFKA_SNOW_CONN_TIMEOUT_MS', 10_000),
      requestTimeout: this.num('KAFKA_SNOW_REQ_TIMEOUT_MS', 30_000),
      enforceRequestTimeout: true,
      ssl: ssl as any,
      ...(sasl ? { sasl } : {}),
    };

    try {
      this.kafka = new Kafka(cfg);
      this.admin = this.kafka.admin();
      this.producer = this.kafka.producer({
        // mantém o comportamento que você já vinha usando
        createPartitioner: Partitioners.LegacyPartitioner,
      });
      this.logger.log('Kafka SN: admin/producer criados.');
    } catch (err) {
      this.logger.error('Kafka SN: falha ao construir client', err as any);
    }
  }

  /** Extrai o primeiro hostname (sem porta) para SNI */
  private firstHostname(brokers: string[]): string | null {
    for (const b of brokers) {
      const [host] = b.split(':');
      if (host && !/^\d{1,3}(\.\d{1,3}){3}$/.test(host)) return host;
    }
    return null;
  }

  private num(env: string, fallback: number): number {
    const v = this.config.get<string>(env);
    const n = v ? Number(v) : NaN;
    return Number.isFinite(n) ? n : fallback;
  }

  private bool(env: string, fallback: boolean): boolean {
    const v = (this.config.get<string>(env) || '').toLowerCase().trim();
    if (!v) return fallback;
    return ['1', 'true', 'yes', 'y', 'on'].includes(v);
  }

  /** SASL opcional (PLAIN) — se usuário/senha vierem. */
  private buildSASL() {
    const user = this.config.get<string>('KAFKA_SNOW_SASL_USERNAME') || '';
    const pass = this.config.get<string>('KAFKA_SNOW_SASL_PASSWORD') || '';
    const mech = (this.config.get<string>('KAFKA_SNOW_SASL_MECHANISM') || 'PLAIN').toLowerCase();

    if (!user || !pass) {
      this.logger.log('Kafka SN: SASL não configurado (ok se o cluster for mTLS-only).');
      return undefined;
    }
    this.logger.log(`Kafka SN: SASL habilitado (mechanism=${mech}).`);
    return { mechanism: mech as any, username: user, password: pass };
  }

  /**
   * Monta opções TLS aceitando:
   * - PEM: CA + client cert/key (+ passphrase)
   * - P12: pfx (+ passphrase) e CA opcional (recomendado)
   * - Sem CA custom: usa trust store do SO
   */
  private buildSSLOptions(servername: string) {
    const caPath  = this.config.get<string>('KAFKA_SNOW_SSL_CA_PATH')   || '';
    const crtPath = this.config.get<string>('KAFKA_SNOW_SSL_CERT_PATH') || '';
    const keyPath = this.config.get<string>('KAFKA_SNOW_SSL_KEY_PATH')  || '';
    const p12Path = this.config.get<string>('KAFKA_SNOW_SSL_P12_PATH')  || '';

    const passphrase =
      this.config.get<string>('KAFKA_SNOW_SSL_KEY_PASSPHRASE') ||
      this.config.get<string>('KAFKA_SNOW_SSL_P12_PASSPHRASE') || '';

    const minVersion: TLSMin = (this.config.get<string>('KAFKA_SNOW_SSL_MIN_VERSION') as TLSMin) || 'TLSv1.2';
    const rejectUnauthorized = this.bool('KAFKA_SNOW_SSL_REJECT_UNAUTHORIZED', true);

    const ssl: any = { servername, rejectUnauthorized, minVersion };

    // Preferência: P12 (keystore) -> PEM (cert/key) -> só CA -> trust do SO
    if (p12Path) {
      this.assertFile(p12Path, 'P12');
      ssl.pfx = fs.readFileSync(p12Path);
      if (passphrase) ssl.passphrase = passphrase;
      if (caPath) {
        this.assertFile(caPath, 'CA');
        ssl.ca = [fs.readFileSync(caPath, 'utf8')];
      }
      this.logger.log(`TLS SN: usando P12 (${path.basename(p12Path)}) + ${caPath ? 'CA custom' : 'CA do SO'}.`);
      return ssl;
    }

    if (crtPath && keyPath) {
      this.assertFile(crtPath, 'CERT');
      this.assertFile(keyPath, 'KEY');
      ssl.cert = fs.readFileSync(crtPath, 'utf8');
      ssl.key  = fs.readFileSync(keyPath, 'utf8');
      if (passphrase) ssl.passphrase = passphrase;
      if (caPath) {
        this.assertFile(caPath, 'CA');
        ssl.ca = [fs.readFileSync(caPath, 'utf8')];
      }
      this.logger.log(`TLS SN: usando PEM (cert/key) + ${caPath ? 'CA custom' : 'CA do SO'}.`);
      return ssl;
    }

    if (caPath) {
      this.assertFile(caPath, 'CA');
      ssl.ca = [fs.readFileSync(caPath, 'utf8')];
      this.logger.log('TLS SN: sem client cert — apenas CA custom aplicada.');
    } else {
      this.logger.log('TLS SN: sem CA custom — usando trust store do sistema.');
    }

    return ssl;
  }

  private assertFile(p: string, label: string) {
    if (!fs.existsSync(p) || !fs.statSync(p).isFile()) {
      throw new Error(`TLS SN: ${label} path inválido: ${p}`);
    }
  }

  async init(timeoutMs = 30_000): Promise<void> {
    if (!this.kafka || !this.admin || !this.producer) {
      this.logger.log('Kafka SN não configurado; pulando init.');
      return;
    }
    if (this.connected || this.initializing) return;

    this.initializing = true;
    const start = Date.now();
    let attempt = 0;

    while (Date.now() - start < timeoutMs) {
      attempt++;
      try {
        this.logger.log(`Kafka SN: conectando (tentativa #${attempt})`);
        await this.admin.connect();
        await this.admin.listTopics(); // sanity check
        await this.producer.connect();
        this.connected = true;
        this.initializing = false;
        this.logger.log('Kafka SN conectado (admin + producer).');
        return;
      } catch (err: any) {
        const msg = String(err?.message || err);
        this.logger.warn(`Kafka SN: falha #${attempt}: ${msg}`);

        // Diagnósticos rápidos
        if (/bad certificate/i.test(msg) || /alert number 42/.test(msg)) {
          this.logger.warn('→ mTLS rejeitado: client cert/clave inválidos ou passphrase errada.');
        } else if (/certificate unknown/i.test(msg) || /alert number 46/.test(msg) || /self[- ]signed/i.test(msg)) {
          this.logger.warn('→ CA/chain do servidor não confiada: aplique KAFKA_SNOW_SSL_CA_PATH com a cadeia correta.');
        } else if (/handshake/i.test(msg) && /no suitable key/i.test(msg)) {
          this.logger.warn('→ Falta de client cert: forneça P12 (KAFKA_SNOW_SSL_P12_PATH) ou PEM (CERT/KEY).');
        }

        try { await this.admin.disconnect(); } catch {}
        try { await this.producer.disconnect(); } catch {}
        await new Promise(r => setTimeout(r, Math.min(attempt * 500, 3000)));
      }
    }

    this.initializing = false;
    const msg = `Timeout ao conectar Kafka SN após ${timeoutMs}ms`;
    this.logger.error(msg);
    throw new Error(msg);
  }

  async sendToServiceNow(topic: string, key: string | null, value: unknown, headers?: Record<string, string>) {
    if (!this.producer) throw new Error('SN producer não configurado');
    if (!this.connected) await this.init(8_000);

    const payload = typeof value === 'string' ? value : JSON.stringify(value);
    await this.producer.send({
      topic,
      messages: [{ key: key ?? null, value: payload, headers }],
      acks: -1, // all replicas
    });
  }

  isConnected(): boolean {
    return this.connected;
  }

  async onModuleDestroy() {
    this.logger.log('ServiceNowKafkaService finalizando');
    try { await this.producer?.disconnect(); } catch (e) { this.logger.warn(e as any); }
    try { await this.admin?.disconnect(); } catch (e) { this.logger.warn(e as any); }
  }
}
