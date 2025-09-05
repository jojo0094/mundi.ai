import React from 'react';

export async function init(): Promise<void> {
  // OSS build: EE features disabled
  // eslint-disable-next-line no-console
  console.log('[OSS] Mundi Public: running without EE features');
}

export function Provider({ children }: React.PropsWithChildren) {
  return <>{children}</>;
}

export function RequireAuth({ children }: React.PropsWithChildren) {
  return <>{children}</>;
}

export function Routes(_reactRouterDom: unknown): React.ReactNode | null {
  return null;
}

export function AccountMenu(): React.ReactNode | null {
  return null;
}

export function ScheduleCallButton(): React.ReactNode | null {
  return null;
}

export function ShareEmbedModal(_props: { isOpen: boolean; onClose: () => void; projectId?: string }): React.ReactNode | null {
  return null;
}

export function ApiKeys(): React.ReactNode | null {
  return null;
}

export async function getJwt(): Promise<string | undefined> {
  return undefined;
}

export function OptionalAuth({ children }: React.PropsWithChildren) {
  return <>{children}</>;
}

export async function fetchMaybeAuth(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
  // OSS build: no auth redirect; just use fetch
  return fetch(input, init);
}
