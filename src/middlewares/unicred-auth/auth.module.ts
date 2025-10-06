import { Module } from '@nestjs/common';
import { AuthRealmController } from './auth.controller';
import { OauthService } from './oauth.service';

@Module({
  controllers: [AuthRealmController],
  providers: [OauthService],
})
export class AuthModule {}
