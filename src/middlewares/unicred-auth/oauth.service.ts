// src/services/oauth/oauth.service.ts
import { Injectable, Logger } from '@nestjs/common';
import axios, { AxiosInstance } from 'axios';
import * as qs from 'qs';
import { ConfigService } from '@nestjs/config';

@Injectable()
export class OauthService {
  private readonly logger = new Logger(OauthService.name);
  private axios: AxiosInstance;
  private cachedToken: string | null = null;
  private tokenExpiresAt: number | null = null; // epoch ms
  private refreshingPromise: Promise<string> | null = null;

  constructor(private readonly config: ConfigService) {
    this.axios = axios.create({
      timeout: Number(this.config.get<number>('UNiCRED_TOKEN_TIMEOUT_MS')) || 5000,
    });
  }

  /**
   * Obtém token OAuth — usa cache e faz refresh antecipado.
   * Retorna apenas o access_token (string). Caller monta o header: `Authorization: Bearer ${token}`
   */
  async getTokenUnicred(): Promise<string> {
    const now = Date.now();

    // Se existe token válido com margem, retorna
    if (this.cachedToken && this.tokenExpiresAt && now + 30_000 < this.tokenExpiresAt) {
      return this.cachedToken;
    }

    // Se já está em processo de refresh, aguarda a promise
    if (this.refreshingPromise) {
      this.logger.debug('Awaiting ongoing token refresh');
      return this.refreshingPromise;
    }

    // Inicia refresh (guarda a promise para concorrentes aguardarem)
    this.refreshingPromise = this.refreshTokenFlow()
      .then(token => {
        this.refreshingPromise = null;
        return token;
      })
      .catch(err => {
        this.refreshingPromise = null;
        throw err;
      });

    return this.refreshingPromise;
  }

  private async refreshTokenFlow(): Promise<string> {
    const unicredAuthUrl = this.config.get<string>('UNICRED_OAUTH_URL');
    if (!unicredAuthUrl) {
      throw new Error('UNICRED_OAUTH_URL not configured');
    }

    const grantType = this.config.get<string>('UNICRED_GRANT_TYPE') || 'password';
    const username = this.config.get<string>('UNICRED_USERNAME') || '';
    const password = this.config.get<string>('UNICRED_PASSWORD') || '';
    const clientSecret = this.config.get<string>('UNICRED_CLIENT_SECRET');
    const clientId = this.config.get<string>('UNICRED_CLIENT_ID');

    // montar payload de acordo com o grant_type configurado
    const payload: any = { grant_type: grantType, client_id: clientId };
    if (clientSecret) payload.client_secret = clientSecret;

    // Se estiver usando password grant (como no seu exemplo)
    if (grantType === 'password') {
      payload.username = username;
      payload.password = password;
    }

    // Se for client_credentials
    if (grantType === 'client_credentials') {
      // possivelmente scopes podem ser necessários
      const scope = this.config.get<string>('UNICRED_OAUTH_SCOPE');
      if (scope) payload.scope = scope;
    }

    const data = qs.stringify(payload);

    try {
      this.logger.log(`Requesting Unicred token (grant=${grantType})`);
      const resp = await this.axios.post(unicredAuthUrl, data, {
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      });

      const body = resp.data;
      if (!body || !body.access_token) {
        this.logger.error('Token response missing access_token', JSON.stringify(body));
        throw new Error('Invalid token response');
      }

      // calcula expiração (em ms)
      const expiresIn = Number(body.expires_in) || Number(this.config.get<number>('UNICRED_TOKEN_DEFAULT_EXPIRES')) || 300;
      const expiresAt = Date.now() + expiresIn * 1000;

      // guarda na memória (cache)
      this.cachedToken = body.access_token;
      this.tokenExpiresAt = expiresAt;

      this.logger.log(`Obtained Unicred token, expires in ${expiresIn}s`);
      return this.cachedToken;
    } catch (err: any) {
      const status = err?.response?.status;
      const dataErr = err?.response?.data;
      this.logger.error(`Error fetching token (status=${status}) — ${err.message}`, dataErr);
      // Lance o erro para o chamador lidar (circuit-breaker, fallback, etc.)
      throw new Error(`Failed to get Unicred token: ${err.message}`);
    }
  }

  /** Força refresh (útil em testes) */
  async forceRefresh(): Promise<string> {
    this.cachedToken = null;
    this.tokenExpiresAt = null;
    return this.getTokenUnicred();
  }
}
