import '../../static/css/main.css';
import '../../static/css/frontend.css';
import '../../static/css/response.css';
import '../../static/css/table.css';

import type { showMap } from '../leafletUtils';
import { getQueryParameter, inversePromise, loader } from '../utils';
import { initializeMap } from './leaflet';
import type { IncomingMessage, OutgoingMessage } from './occurrence';
import { dispatch } from './occurrence';

const occId = getQueryParameter('occid', (occId) => occId.length > 0);
const nameString = getQueryParameter(
  'namestr',
  (nameString) => nameString.length > 0
);
const queryString = `?occid=${occId}&namestr=${nameString}&loader=false`;
const fetchUrl = `${window.location.origin}${window.location.pathname}${queryString}${window.location.hash}`;

const version = '1.0.0';
const origin = getQueryParameter('origin', (origin) =>
  origin.startsWith('http')
);
export const [resolveMap, getMap] =
  inversePromise<Readonly<[L.Map, L.Control.Layers, HTMLElement]>>();
const sendMessage = (action: OutgoingMessage) =>
  window.opener?.postMessage(action, origin);

loader(
  async () =>
    Promise.all([
      fetch(fetchUrl).then(async (response) => response.text()),
      new Promise<''>((resolve) => {
        if (!origin || !window.opener) {
          resolve('');
          return;
        }
        sendMessage({ type: 'LoadedAction', version });
        const messageHandler = (action: IncomingMessage): void =>
          dispatch({
            ...action,
            state: {
              sendMessage,
            },
          });
        window.addEventListener('message', (event) => {
          if (
            event.source !== window.opener ||
            event.origin !== origin ||
            typeof event.data !== 'object' ||
            typeof event.data.type !== 'string'
          )
            return;
          if (event.data.type === 'BasicInformationAction') resolve('');
          messageHandler(event.data);
        });
      }),
    ]).then(([innerHtml]) => innerHtml),
  () => {
    initializeMap();
    getMap().then(([_map, _layerGroup, mapContainer]) => {
      const popUpPane =
        mapContainer.getElementsByClassName('leaflet-popup-pane')[0];
      if (!popUpPane) throw new Error('Unable to find the pop up pane');
      popUpPane.addEventListener('click', (event) => {
        if (!event.target) return;
        const button = event.target as HTMLElement;
        if (button.tagName !== 'BUTTON') return;
        const index = button.getAttribute('data-index');
        if (!index) return;
        sendMessage({
          type: 'ViewRecordAction',
          index: Number.parseInt(index),
        });
      });
    });
  }
);

document.body.addEventListener('click', (event) => {
  if (!event.target) return;

  const target = event.target as HTMLElement;

  const dictionaryLabel = target.closest('.dictionary-label');
  if (dictionaryLabel !== null) {
    dictionaryLabel.parentElement?.classList.toggle('collapsed');
    return;
  }
  const collapsed = target.closest('.collapsed');
  if (collapsed !== null) collapsed.classList.toggle('collapsed');
});
