import type {
  BindingModeConfig,
  Container,
  Monitor,
  RunCommandResponse,
  TilingDirection,
  Window,
} from 'glazewm';
import { z } from 'zod';

import { desktopCommands, getMonitors, onProviderEmit } from '~/desktop';
import { getCoordinateDistance } from '~/utils';
import { createBaseProvider } from '../create-base-provider';
import type {
  GlazeWmProvider,
  GlazeWmProviderConfig,
} from './glazewm-provider-types';

const glazeWmProviderConfigSchema = z.object({
  type: z.literal('glazewm'),
});

/**
 * Raw GlazeWM state from the backend.
 *
 * GlazeWM's IPC server is connected to from the backend, since it rejects
 * connections from the webview.
 */
interface GlazeWmResponse {
  allMonitors: Monitor[];
  allWindows: Window[];
  focusedContainer: Container;
  bindingModes: BindingModeConfig[];
  tilingDirection: TilingDirection;
  isPaused: boolean;
}

export function createGlazeWmProvider(
  config: GlazeWmProviderConfig,
): GlazeWmProvider {
  const mergedConfig = glazeWmProviderConfigSchema.parse(config);

  // TODO: Update state when monitors change.
  return createBaseProvider(mergedConfig, async queue => {
    const monitors = await getMonitors();

    function getMonitorState(res: GlazeWmResponse) {
      const currentPosition = {
        x: monitors.currentMonitor!.x,
        y: monitors.currentMonitor!.y,
      };

      // Get GlazeWM monitor that corresponds to the widget's monitor.
      const currentGlazeWmMonitor = res.allMonitors.reduce((a, b) =>
        getCoordinateDistance(currentPosition, a) <
        getCoordinateDistance(currentPosition, b)
          ? a
          : b,
      );

      const focusedGlazeWmMonitor = res.allMonitors.find(
        monitor => monitor.hasFocus,
      );

      const allGlazeWmWorkspaces = res.allMonitors.flatMap(
        monitor => monitor.children,
      );

      const focusedGlazeWmWorkspace = focusedGlazeWmMonitor?.children.find(
        workspace => workspace.hasFocus,
      );

      const displayedGlazeWmWorkspace =
        currentGlazeWmMonitor.children.find(
          workspace => workspace.isDisplayed,
        );

      return {
        displayedWorkspace: displayedGlazeWmWorkspace!,
        focusedWorkspace: focusedGlazeWmWorkspace!,
        currentWorkspaces: currentGlazeWmMonitor.children,
        allWorkspaces: allGlazeWmWorkspaces,
        focusedMonitor: focusedGlazeWmMonitor!,
        currentMonitor: currentGlazeWmMonitor,
        allMonitors: res.allMonitors,
        allWindows: res.allWindows,
      };
    }

    return onProviderEmit<GlazeWmResponse>(
      mergedConfig,
      ({ configHash, result }) => {
        if ('error' in result) {
          queue.error(result.error);
          return;
        }

        queue.output({
          ...getMonitorState(result.output),
          focusedContainer: result.output.focusedContainer,
          tilingDirection: result.output.tilingDirection,
          bindingModes: result.output.bindingModes,
          isPaused: result.output.isPaused,
          runCommand: (command: string, subjectContainerId?: string) => {
            return desktopCommands.callProviderFunction<RunCommandResponse>(
              configHash,
              {
                type: 'glazewm',
                function: {
                  name: 'run_command',
                  args: { command, subjectContainerId },
                },
              },
            );
          },
        });
      },
    );
  });
}
