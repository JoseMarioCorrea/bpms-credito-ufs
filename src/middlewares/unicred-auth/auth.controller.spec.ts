import { Test, TestingModule } from '@nestjs/testing';
import { AuthRealmController } from './auth.controller'; // Ajuste o caminho do arquivo
import { OauthService } from './oauth.service';
describe('AuthRealmController', () => {
  let authRealmController: AuthRealmController;
  let oauthService: OauthService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [AuthRealmController],
      providers: [
        {
          provide: OauthService,
          useValue: {
            getTokenUnicred: jest.fn(),
          },
        },
      ],
    }).compile();

    authRealmController = module.get<AuthRealmController>(AuthRealmController);
    oauthService = module.get<OauthService>(OauthService);
  });

  it('should be defined', () => {
    expect(authRealmController).toBeDefined();
  });

  describe('getToken', () => {
    it('should return a token when getTokenUnicred is successful', async () => {
      const mockToken = 'mocked_token';
      jest.spyOn(oauthService, 'getTokenUnicred').mockResolvedValue(mockToken);

      const result = await authRealmController.getToken();
      expect(oauthService.getTokenUnicred).toHaveBeenCalled();
      expect(result).toEqual({ token: mockToken });
    });

    it('should return an error message when getTokenUnicred fails', async () => {
      const mockError = new Error('Failed to get token');
      jest.spyOn(oauthService, 'getTokenUnicred').mockRejectedValue(mockError);

      const result = await authRealmController.getToken();
      expect(oauthService.getTokenUnicred).toHaveBeenCalled();
      expect(result).toEqual({ message: 'Erro ao obter token', error: mockError.message });
    });
  });
});
