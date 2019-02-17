import {Injectable} from "@angular/core";
import {BehaviorSubject, Observable, Subject} from "rxjs";
import {Queue} from "../models/queue";

export enum WebSocketStatus {
  IDLE,
  OPEN,
  CLOSE,
  ERROR,
  DISCONNECTED
}

@Injectable({
  providedIn: 'root'
})
export class WebSocketService {
  private ws: WebSocket;

  private status: BehaviorSubject<WebSocketStatus>;
  private currentStatus: WebSocketStatus;

  private outgoing: Queue<string>;
  private incoming: Subject<any>;

  public status$: Observable<WebSocketStatus>;
  public incoming$: Observable<any>;

  constructor() {
    this.status = new BehaviorSubject(WebSocketStatus.IDLE);
    this.status$ = this.status.asObservable();

    this.incoming = new Subject();
    this.incoming$ = this.incoming.asObservable();

    this.outgoing = new Queue(10);

    this.currentStatus = WebSocketStatus.IDLE;

    this.connect();
  }

  public send(msg: any) {
    const m = JSON.stringify(msg);

    if (this.currentStatus === WebSocketStatus.OPEN) {
      this.ws.send(m);
    } else {
      this.outgoing.offer(m);
    }
  }

  public connect() {
    if (this.currentStatus === WebSocketStatus.OPEN) {
      return;
    }

    this.ws = new WebSocket('ws://localhost:8080/ws');
    this.ws.onopen = () => this.onOpen();
    this.ws.onclose = () => this.onClose();
    this.ws.onerror = () => this.onError();
    this.ws.onmessage = (ev) => this.onMessage(ev);
  }

  public disconnect() {
    if (this.currentStatus === WebSocketStatus.IDLE || this.currentStatus === WebSocketStatus.DISCONNECTED) {
      return;
    }

    this.currentStatus = WebSocketStatus.DISCONNECTED;
    this.ws.close();
    this.status.next(WebSocketStatus.DISCONNECTED);
  }

  private onOpen() {
    this.currentStatus = WebSocketStatus.OPEN;
    console.log('WS Connected');
    this.status.next(WebSocketStatus.OPEN);
    this.outgoing.forEach(m => this.ws.send(m));
  }

  private onClose() {
    if (this.currentStatus === WebSocketStatus.DISCONNECTED) {
      return;
    }

    this.currentStatus = WebSocketStatus.CLOSE;
    this.status.next(WebSocketStatus.CLOSE);
    setTimeout(() => this.connect(), 5000);
  }

  private onError() {
    if (this.currentStatus === WebSocketStatus.DISCONNECTED) {
      return;
    }

    this.currentStatus = WebSocketStatus.ERROR;
    this.status.next(WebSocketStatus.ERROR);
  }

  private onMessage(ev) {
    this.incoming.next(JSON.parse(ev.data));
  }
}
