export {};

declare global {
	namespace ioBroker {
		interface AdapterConfig {
			useIpv6: boolean;
			ipV4: string;
			ipV6: string;
			username: string;
			password: string;
			pollInterval: number;
		}
	}
}
