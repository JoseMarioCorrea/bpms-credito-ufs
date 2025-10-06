import { Injectable, NestInterceptor, ExecutionContext, CallHandler } from '@nestjs/common';
import { Observable, tap } from 'rxjs';

@Injectable()
export class LoggerInterceptor implements NestInterceptor {
  intercept(context: ExecutionContext, next: CallHandler): Observable<any> {
    const now = Date.now();
    return next.handle().pipe(tap(() => {
      // hook pra metrics/traces
      const elapsed = Date.now() - now;
      console.log(`[LOG] ${context.getClass().name}.${context.getHandler().name} ${elapsed}ms`);
    }));
  }
  
}
