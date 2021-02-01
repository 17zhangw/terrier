#include "self_driving/pilot/pilot_thread.h"

namespace noisepage::selfdriving {
PilotThread::PilotThread(common::ManagedPointer<Pilot> pilot, std::chrono::microseconds pilot_period,
                         std::chrono::microseconds forecaster_train_period, bool pilot_planning)
    : pilot_(pilot),
      run_pilot_(true),
      pilot_paused_(!pilot_planning),
      pilot_period_(pilot_period),
      forecaster_train_period_(forecaster_train_period),
      forecaster_remain_period_(forecaster_train_period),
      pilot_thread_(std::thread([this] { PilotThreadLoop(); })) {}

}  // namespace noisepage::selfdriving
