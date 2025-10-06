import { Controller, Get } from '@nestjs/common';
import { OauthService } from './oauth.service';
import { ApiTags, ApiOperation, ApiResponse } from '@nestjs/swagger';

@ApiTags('Auth Unicred')  // Adiciona a tag "Auth" no Swagger
@Controller('auth/realm')
export class AuthRealmController {
  constructor(private readonly oauthService: OauthService) {}

  @Get('token')
  @ApiOperation({ summary: 'Obter token de autenticação' })  // Descrição da operação
  @ApiResponse({ status: 200, description: 'Token retornado com sucesso' })  // Resposta de sucesso
  @ApiResponse({ status: 500, description: 'Erro ao obter o token' })  // Resposta de erro
  async getToken() {
    try {
      const token = await this.oauthService.getTokenUnicred();
      return { token };
    } catch (error) {
      return { message: 'Erro ao obter token', error: error.message };
    }
  }
}
