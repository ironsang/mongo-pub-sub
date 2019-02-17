import {Component, OnInit} from '@angular/core';
import {WebSocketService} from "./services/web-socket.service";
import {filter} from "rxjs/operators";

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html'
})
export class AppComponent implements OnInit{

  eventStatus = {
    AddNewField: false,
    EditField: false,
    DeleteField: false
  };

  public constructor(private webSocketService: WebSocketService) {
  }

  sendEvent(eventName: string) {
    this.eventStatus[eventName] = false;

    const message = {
      type: 'COMMAND',
      headers: {
        command: 'PUBLISH',
        topic: 'events'
      },
      content: {
        name: eventName,
        user: 'demo-user',
        eventReference: 'demo-db',
        eventTime: Date.now(),
        processed: false
      }
    };

    this.webSocketService.send(message);
    console.log(message);
  }

  ngOnInit(): void {
    this.webSocketService.send({
      type: 'COMMAND',
      headers: {
        command: 'SUBSCRIBE',
        topic: 'events'
      },
      content: {
        filter: {
          processed: true
        }
      }
    });

    this.webSocketService.incoming$.subscribe(d => console.log(d));

    this.webSocketService.incoming$.pipe(
      filter(d => d.type === 'MESSAGE'),
    ).subscribe(d => this.eventStatus[d.content.name] = d.content.processed);
  }
}
