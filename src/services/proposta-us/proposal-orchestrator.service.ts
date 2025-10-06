// src/proposal-orchestrator/proposal-orchestrator.service.ts
import { Injectable } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom } from 'rxjs';

@Injectable()
export class ProposalOrchestratorService {
  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
  ) { }

  // Helper para executar chamadas GET com headers customizados.
  // Caso ocorra um erro (ex: 404), retorna um array vazio.
  private async fetchData(url: string, headers: Record<string, string>): Promise<any> {
    try {
      const response = await firstValueFrom(
        this.httpService.get(url, { headers })
      );
      return response.data;
    } catch (error: any) {
      // Se o status do erro for 404, ou qualquer outro, retorna array vazio
      if (error?.response?.status === 404) {
        return ['Não localizado Proposta'];
      }
      // Para outros tipos de erro, você pode logar ou tratar conforme necessário,
      // mas neste caso, retornamos array vazio para não lançar exceção.
      return [];
    }
  }

  async getProposta(numeroProposta: number, headers: Record<string, string>): Promise<any> {
    const baseUrlProposta = this.configService.get<string>('BASEURL_PROPOSTA_US');
    const url = `${baseUrlProposta}/credito/proposta/v2/propostas/${numeroProposta}`;
    console.log('headers: ', headers);
    console.log('url: ', url);
    const response = await this.fetchData(url, headers);
    console.log('response: ', JSON.stringify(response));
    return response;
  }

  async getTipoVencimento(headers: Record<string, string>): Promise<any> {
    const baseUrlProposta = this.configService.get<string>('BASEURL_PROPOSTA_US');
    const url = `${baseUrlProposta}/credito/proposta/v2/propostas/tipos-vencimentos`;
    return this.fetchData(url, headers);
  }

  async getContaCorrente(numeroConta: number, headers: Record<string, string>): Promise<any> {
    const baseUrlConta = this.configService.get<string>('BASEURL_CONTA_CORRENTE_US');
    const url = `${baseUrlConta}/conta-corrente/conta-corrente/v1/contas-correntes/${numeroConta}`;
    console.log('url: ', url)
    return this.fetchData(url, headers);
  }

  async getPessoaFisica(cpf: string, headers: Record<string, string>): Promise<any> {
    const baseUrlPessoa = this.configService.get<string>('BASEURL_PESSOA_US');
    const url = `${baseUrlPessoa}/cadastro/pessoa-fisica/v1/pessoa-fisica/${cpf}`;
    return this.fetchData(url, headers);
  }

  async getPessoaJuridica(cnpj: string, headers: Record<string, string>): Promise<any> {
    const baseUrlPessoa = this.configService.get<string>('BASEURL_PESSOA_US');
    const url = `${baseUrlPessoa}/cadastro/pessoa-juridica/v1/pessoa-juridica/${cnpj}`;
    return this.fetchData(url, headers);
  }

  async getLinhaCredito(idLinha: number, headers: Record<string, string>): Promise<any> {
    const baseUrlLinhaCredito = this.configService.get<string>('BASEURL_LINHA_CREDITO_US');
    const url = `${baseUrlLinhaCredito}/credito/v2/linhas-credito/${idLinha}`;
    return this.fetchData(url, headers);
  }

  async getSeguroPrestamista(cpfCnpj: string, numeroProposta: number, headers: Record<string, string>): Promise<any> {
    const baseUrlSeguro = this.configService.get<string>('BASEURL_SEGURO_PRESTAMISTA_US');
    const url = `${baseUrlSeguro}/seguro-prestamista/v1/segurados/${cpfCnpj}/propostas/${numeroProposta}`;
    return this.fetchData(url, headers);
  }

  async getAvalistas(numeroProposta: number, headers: Record<string, string>): Promise<any> {
    const baseUrlProposta = this.configService.get<string>('BASEURL_PROPOSTA_US');
    const url = `${baseUrlProposta}/credito/garantia/v2/garantias/${numeroProposta}/avalistas`;
    return this.fetchData(url, headers);
  }

  async getVeiculosAlienados(numeroProposta: number, headers: Record<string, string>): Promise<any> {
    const baseUrlProposta = this.configService.get<string>('BASEURL_PROPOSTA_US');
    const url = `${baseUrlProposta}/credito/garantia/v2/garantias/${numeroProposta}/veiculos-alienados`;
    return this.fetchData(url, headers);
  }

  async getImoveisAlienados(numeroProposta: number, headers: Record<string, string>): Promise<any> {
    const baseUrlProposta = this.configService.get<string>('BASEURL_PROPOSTA_US');
    const url = `${baseUrlProposta}/credito/garantia/v2/garantias/${numeroProposta}/imoveis-alienados`;
    return this.fetchData(url, headers);
  }

  async getAplicacoesFinanceiras(numeroProposta: number, headers: Record<string, string>): Promise<any> {
    const baseUrlProposta = this.configService.get<string>('BASEURL_PROPOSTA_US');
    const url = `${baseUrlProposta}/credito/garantia/v2/garantias/${numeroProposta}/aplicacoes-financeiras`;
    return this.fetchData(url, headers);
  }

  async getOutrasGarantias(numeroProposta: number, headers: Record<string, string>): Promise<any> {
    const baseUrlProposta = this.configService.get<string>('BASEURL_PROPOSTA_US');
    const url = `${baseUrlProposta}/credito/garantia/v2/garantias/${numeroProposta}/outras-garantias`;
    return this.fetchData(url, headers);
  }

  /**
  * Monta a proposta completa a partir do numeroProposta.
  * Faz chamadas em paralelo e agrupa resultados (tolerando falhas parciais).
  */
  async buildFullProposal(numeroProposta: number, headers: Record<string, string> = {}): Promise<any> {
    // Primeiro pegamos a proposta minimal para extrair dados como cpf/cnpj, conta, linhaCredito
    const proposta = await this.getProposta(numeroProposta, headers).catch(e => null);

    // extrair campos úteis se existir proposta
    const cpfCnpj = proposta?.cpfCnpj || proposta?.titular?.cpfCnpj || null;
    const conta = proposta?.conta || proposta?.contaCorrente || null;
    const idLinha = proposta?.idLinhaCredito || proposta?.linhaCredito || null;

    // chamadas em paralelo (fazer await allSettled para tolerância)
    const calls = {
      tiposVencimento: this.getTipoVencimento(headers),
      linhaCredito: idLinha ? this.getLinhaCredito(idLinha, headers) : Promise.resolve([]),
      avalistas: this.getAvalistas(numeroProposta, headers),
      veiculos: this.getVeiculosAlienados(numeroProposta, headers),
      imoveis: this.getImoveisAlienados(numeroProposta, headers),
      aplicacoes: this.getAplicacoesFinanceiras(numeroProposta, headers),
      outrasGarantias: this.getOutrasGarantias(numeroProposta, headers),
      contaCorrente: conta ? this.getContaCorrente(Number(conta), headers) : Promise.resolve(null),
      pessoaFisica: cpfCnpj && cpfCnpj.length <= 11 ? this.getPessoaFisica(cpfCnpj, headers) : Promise.resolve(null),
      pessoaJuridica: cpfCnpj && cpfCnpj.length > 11 ? this.getPessoaJuridica(cpfCnpj, headers) : Promise.resolve(null),
      seguroPrestamista: cpfCnpj ? this.getSeguroPrestamista(cpfCnpj, numeroProposta, headers) : Promise.resolve(null),
    };

    const settled = await Promise.allSettled(Object.values(calls));
    const keys = Object.keys(calls);

    const results: Record<string, any> = {};
    for (let i = 0; i < keys.length; i++) {
      const k = keys[i];
      const s = settled[i];
      results[k] = s.status === 'fulfilled' ? s.value : { error: s.reason?.message || String(s.reason) };
    }

    // Monta estrutura final (ajuste conforme seu contrato)
    const full = {
      proposta: proposta || null,
      tiposVencimento: results.tiposVencimento,
      linhaCredito: results.linhaCredito,
      avalistas: results.avalistas,
      veiculosAlienados: results.veiculos,
      imoveisAlienados: results.imoveis,
      aplicacoesFinanceiras: results.aplicacoes,
      outrasGarantias: results.outrasGarantias,
      contaCorrente: results.contaCorrente,
      pessoaFisica: results.pessoaFisica,
      pessoaJuridica: results.pessoaJuridica,
      seguroPrestamista: results.seguroPrestamista,
      meta: {
        assembledAt: new Date().toISOString(),
        source: 'proposal-orchestrator',
        numeroProposta,
      },
    };

    return full;
  }
}
