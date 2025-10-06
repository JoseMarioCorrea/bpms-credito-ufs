import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { lastValueFrom } from 'rxjs';
import { ConfigService } from '@nestjs/config';

@Injectable()
export class ContaUsService {
  private readonly logger = new Logger(ContaUsService.name);
  private readonly baseUrl: string;
  private readonly urlGerenteConta: string;

  constructor(
    private readonly configService: ConfigService,
    private readonly httpService: HttpService,
  ) {
    this.baseUrl = this.configService.get<string>('BASE_URL_UNICRED');
    this.urlGerenteConta = this.configService.get<string>('URL_CONTAS');
  }

  async consultarGerenteConta(conta: number, cooperativa: string, token: string): Promise<any> {
    const url = `${this.baseUrl}/${this.urlGerenteConta}/v1/${conta}/gerente`;
    const headers = {
      Accept: '*/*',
      cooperativa,
      token: `${token}`,
      sau: true,
    };

    try {
      const response = await lastValueFrom(this.httpService.get(url, { headers }));
      const data = response.data;

      this.logger.log(`Gerente da conta ${conta}: ${JSON.stringify(data)}`);
      return data;
    } catch (error) {
      const statusCode = error.response?.status;
      this.logger.error(
        `Erro ao consultar gerente da conta (Conta: ${conta}, Cooperativa: ${cooperativa}, Status: ${statusCode}): ${error.message}`,
      );
      return {
        success: false,
        message: statusCode === 500
          ? 'Erro interno no serviço de consulta de gerente. Tente novamente mais tarde.'
          : 'Erro ao consultar gerente da conta.',
        errorDetails: error.response?.data || error.message,
      };
    }
  }
}
