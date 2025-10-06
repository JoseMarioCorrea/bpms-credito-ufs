import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { lastValueFrom } from 'rxjs';
import { ConfigService } from '@nestjs/config';

@Injectable()
export class ContaService {
  private readonly logger = new Logger(ContaService.name);
  private readonly baseUrl: string;
  private readonly urlConta: string;
  private readonly urlGerenteConta: string;

  constructor(
    private readonly configService: ConfigService,
    private readonly httpService: HttpService,
  ) {
    this.baseUrl = this.configService.get<string>('BASE_URL_UNICRED');
    this.urlConta = this.configService.get<string>('URL_CONTA');
  }

  async consultarConta(conta: number, cooperativa: string, token: string): Promise<any> {
    const url = `${this.baseUrl}/${this.urlConta}/${conta}`;
    console.log('URL: ', url)
    const headers = {
      Accept: '*/*',
      cooperativa,
      Authorization: `${token}`,
    };

    try {
      const response = await lastValueFrom(this.httpService.get(url, { headers }));
      const data = response.data;

      if (data.titular) {
        data.titular = data.titular.trim();
      }

      this.logger.log(`Dados da conta obtidos: ${JSON.stringify(data)}`);
      return data;
    } catch (error) {
      const statusCode = error.response?.status;
      this.logger.error(
        `Erro ao consultar conta (Conta: ${conta}, Cooperativa: ${cooperativa}, Status: ${statusCode}): ${error.message}`,
      );
      return {
        success: false,
        message: statusCode === 500
          ? 'Erro interno no serviço de consulta de conta. Tente novamente mais tarde.'
          : 'Erro ao consultar a conta.',
        errorDetails: error.response?.data || error.message,
      };
    }
  }
}
